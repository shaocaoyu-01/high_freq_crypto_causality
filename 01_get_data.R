library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)

get_binance_full_history <- function(symbol = "BTCUSDT", start_date = "2022-01-01", interval = "1h") {
  
  base_url <- "https://api.binance.com/api/v3/klines"
  
  # 1. 处理时间戳：R的日期转为Unix毫秒时间戳 (Binance要求)
  # start_date 需要转为 UTC 时间
  current_start <- as.numeric(as.POSIXct(start_date, tz = "UTC")) * 1000
  
  # 获取当前时间作为结束点
  end_time <- as.numeric(Sys.time()) * 1000
  
  # 用于存储所有数据的列表
  all_data_list <- list()
  page_count <- 1
  
  cat("开始抓取", symbol, "数据，起始日期:", start_date, "...\n")
  
  # 2. 开始循环抓取
  repeat {
    # 发送请求
    res <- GET(base_url, query = list(
      symbol = symbol,
      interval = interval,
      limit = 1000,
      startTime = sprintf("%.0f", current_start) # 防止科学计数法
    ))
    
    # 解析数据
    data_content <- content(res, as = "text", encoding = "UTF-8")
    json_data <- fromJSON(data_content)
    
    # 检查是否抓取到数据
    if (length(json_data) == 0) {
      break
    }
    
    # 转换为 DataFrame 并选择前6列
    # 列定义: 1:Open time, 2:Open, 3:High, 4:Low, 5:Close, 6:Volume
    df_chunk <- as.data.frame(json_data) %>% select(1:6)
    colnames(df_chunk) <- c("timestamp", "Open", "High", "Low", "Close", "Volume")
    
    # 存入列表
    all_data_list[[page_count]] <- df_chunk
    
    # 获取本批次最后一条的时间戳
    last_time <- as.numeric(df_chunk$timestamp[nrow(df_chunk)])
    
    # 打印进度
    last_date_readable <- as_datetime(last_time / 1000)
    cat("已获取第", page_count, "页，当前数据截止至:", as.character(last_date_readable), "\n")
    
    # 3. 更新下一次的起始时间
    # 逻辑：最后一条K线的时间 + 1个interval (1小时 = 3600000毫秒)
    # 或者直接利用API特性，取最后一条时间戳 + 1 即可，API会自动找下一个
    interval_ms <- 3600000 # 1小时的毫秒数
    current_start <- last_time + interval_ms
    
    # 如果当前起始时间已经超过了现在，就停止
    if (current_start > end_time) {
      break
    }
    
    page_count <- page_count + 1
    
    # 礼貌性暂停（虽然币安限流很宽，但加上更安全）
    Sys.sleep(0.2)
  }
  
  cat("抓取完成，正在合并数据...\n")
  
  # 4. 合并所有数据块
  full_df <- bind_rows(all_data_list)
  
  # 5. 数据清洗与格式转换
  full_df <- full_df %>%
    mutate(
      Date = as_datetime(as.numeric(timestamp) / 1000),
      Open = as.numeric(Open),
      High = as.numeric(High),
      Low = as.numeric(Low),
      Close = as.numeric(Close),
      Volume = as.numeric(Volume)
    ) %>%
    select(Date, Open, High, Low, Close, Volume) %>%
    distinct(Date, .keep_all = TRUE) %>% # 去重，防止边界重复
    arrange(Date)
  
  return(full_df)
}

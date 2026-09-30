from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2023-07-01") & (F.col("page").isin(["/product", "/checkout"]) | (F.to_date("event_time") >= "2025-03-29")))
)

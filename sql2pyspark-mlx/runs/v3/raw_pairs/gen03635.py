from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id").isin([7, 8, 18]) & (F.to_date("event_time") >= "2025-04-01")) | F.col("page").isin(["/search", "/product"]))
    .select("session_id")
)

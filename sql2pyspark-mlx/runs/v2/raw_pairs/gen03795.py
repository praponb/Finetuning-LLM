from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 20) & F.col("page").isin(["/search", "/home", "/product"]))
    .select("event_type", "event_time", "duration_sec")
)

from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2025-01-01")
    .groupBy("event_type")
    .pivot("page", ["/cart", "/search", "/home"])
    .agg(F.max("duration_sec"))
    .orderBy("event_type")
)

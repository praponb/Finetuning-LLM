from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2025-09-01")
    .groupBy("page")
    .pivot("event_type", ["purchase", "search", "click"])
    .agg(F.sum("duration_sec"))
    .orderBy("page")
)

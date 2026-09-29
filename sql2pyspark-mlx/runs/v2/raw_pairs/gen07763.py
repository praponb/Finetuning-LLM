from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 23, 28, 36]))
    .groupBy("event_type")
    .pivot("page", ["/product", "/search"])
    .agg(F.sum("duration_sec"))
)

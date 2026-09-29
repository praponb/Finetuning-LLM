from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .pivot("event_type", ["click", "purchase"])
    .agg(F.sum("duration_sec").alias("value_sum"))
)

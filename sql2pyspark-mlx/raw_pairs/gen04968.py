from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "session_id")
    .pivot("event_type", ["page_view", "purchase"])
    .agg(F.sum("duration_sec").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)

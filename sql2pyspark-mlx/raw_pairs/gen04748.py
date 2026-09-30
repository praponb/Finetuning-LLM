from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 524, "high").when(F.col("duration_sec") >= 211, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.min("event_time").alias("min_event_time"))
    .orderBy("min_event_time", "duration_sec_bucket")
)

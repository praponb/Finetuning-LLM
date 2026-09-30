from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 536, "high").when(F.col("duration_sec") >= 198, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.max("event_time").alias("last_event_time"))
    .orderBy("duration_sec_bucket", "event_type")
)

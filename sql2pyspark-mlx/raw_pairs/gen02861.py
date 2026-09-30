from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(9, 17))
    .groupBy(
        F.when(F.col("duration_sec") >= 482, "high").when(F.col("duration_sec") >= 138, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.max("event_time").alias("last_event_time"))
)

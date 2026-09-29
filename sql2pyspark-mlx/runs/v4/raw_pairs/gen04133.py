from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2024-07-30") & F.col("session_id").isin([16, 17, 29, 30]) & (F.col("session_id") == 19))
    .groupBy(
        F.when(F.col("duration_sec") >= 440, "high").when(F.col("duration_sec") >= 214, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
    .filter(F.col("stddev_duration_sec") > 35)
    .orderBy("stddev_duration_sec", "duration_sec_bucket")
    .limit(10)
)

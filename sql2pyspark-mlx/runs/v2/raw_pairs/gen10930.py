from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2023-08-01") | F.col("duration_sec").between(291, 344))
    .groupBy(
        F.when(F.col("duration_sec") >= 406, "high").when(F.col("duration_sec") >= 109, "mid").otherwise("low").alias("duration_sec_bucket"),
        "session_id",
        "event_type",
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("total_duration_sec") > 1000)
    .orderBy(
        F.col("last_event_time").desc(),
        "duration_sec_bucket",
        "session_id",
        "event_type",
    )
    .limit(3)
)

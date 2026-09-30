from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2024-02-01")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .orderBy(F.col("min_duration_sec").desc())
)

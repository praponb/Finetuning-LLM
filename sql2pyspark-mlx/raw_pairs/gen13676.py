from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 231)
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") < 1800)
    .drop("total_duration_sec")
)

from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") < 130)
    .select("event_cohort", "max_duration_sec")
)

from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "event_type",
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
)

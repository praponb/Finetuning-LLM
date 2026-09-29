from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(F.min("event_time").alias("first_event_time"))
    .orderBy(F.col("event_cohort").desc(), "first_event_time")
    .limit(1)
)

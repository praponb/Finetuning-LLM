from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "page",
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 180)
    .select("page", "event_cohort", "avg_duration_sec")
    .orderBy(F.col("page").desc(), "event_cohort")
    .limit(10)
)

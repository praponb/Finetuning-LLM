from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
        "page",
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
    )
    .orderBy("page", "event_cohort")
    .limit(5)
)

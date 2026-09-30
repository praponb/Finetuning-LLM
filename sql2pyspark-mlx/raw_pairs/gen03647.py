from pyspark.sql import functions as F

base = df_web_events.filter(F.col("page") == "/cart")

summary = (
    base
    .groupBy(
        F.month("event_time").alias("event_month"),
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("total_duration_sec") >= 2200)
    .orderBy(F.col("total_duration_sec").desc(), "event_cohort")
)

from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
    )
    .agg(
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
        F.sum(F.when(F.col("we.event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .filter(F.col("click_count") >= 74)
    .orderBy(F.col("avg_duration_sec").desc(), "started_cohort")
    .limit(50)
)

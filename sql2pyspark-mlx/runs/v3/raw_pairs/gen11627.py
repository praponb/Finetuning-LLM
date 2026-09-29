from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.page") != "/home")
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
    )
    .agg(
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
    )
    .filter(F.col("min_duration_sec") >= 320)
    .orderBy("min_duration_sec")
)

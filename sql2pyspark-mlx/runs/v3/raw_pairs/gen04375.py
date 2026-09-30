from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
        F.when(F.col("s.browser").isin(["Firefox", "Chrome"]), "core").otherwise("other").alias("browser_group"),
    )
    .agg(F.max(F.col("we.event_time")).alias("latest_event_time"))
)

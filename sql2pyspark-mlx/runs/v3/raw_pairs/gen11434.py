from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.duration_sec") < 360)
    .groupBy(
        F.when(F.year(F.col("s.started_at")) < 2024, "before_2024").when(F.year(F.col("s.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
        F.when(F.col("s.country").isin(["UK", "SG"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
        F.min(F.col("we.event_time")).alias("min_event_time"),
    )
)

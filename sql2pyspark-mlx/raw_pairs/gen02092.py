from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.browser").alias("browser"),
        F.col("s.country").alias("country"),
        F.col("src.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.duration_sec")).alias("min_duration_sec"),
    )
)

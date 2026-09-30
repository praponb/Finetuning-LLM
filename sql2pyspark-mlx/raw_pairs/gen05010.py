from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.browser").alias("browser"),
        F.col("a.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.duration_sec")).alias("min_duration_sec"),
    )
)

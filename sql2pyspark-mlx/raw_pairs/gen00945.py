from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("dim"), F.col("a.session_id") == F.col("dim.id"), "left")
    .rollup(
        F.col("dim.device").alias("device"),
        F.col("dim.country").alias("country"),
        F.col("a.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.duration_sec")).alias("min_duration_sec"),
        F.grouping_id().alias("gid"),
    )
)

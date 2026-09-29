from pyspark.sql import functions as F

by_dimension = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("dim"), F.col("e.session_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.device").alias("device"),
        F.col("e.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.duration_sec")).alias("total_duration_sec"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select("event_type", "rows_total", "total_duration_sec")
    .orderBy(F.col("total_duration_sec").desc(), F.col("event_type").desc())
)

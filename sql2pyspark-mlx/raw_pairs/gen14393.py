from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.device").alias("device"), F.col("p.browser").alias("browser"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.duration_sec")), 2).alias("avg_duration_sec"),
        F.grouping_id().alias("grouping_level"),
    )
)

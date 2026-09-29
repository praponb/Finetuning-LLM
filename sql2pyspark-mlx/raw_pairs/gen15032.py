from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.browser").alias("browser"), F.col("a.page").alias("page"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy("avg_duration_sec")
)

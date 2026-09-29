from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.year(F.col("s.started_at")).alias("started_year"), F.col("s.country"))
    .agg(
        F.count("*").alias("row_count"),
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
    )
    .orderBy("country")
)

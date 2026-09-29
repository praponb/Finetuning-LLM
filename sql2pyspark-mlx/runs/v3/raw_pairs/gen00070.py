from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.customer_id").isin([19, 20, 22, 26]))
    .groupBy(F.col("s.country"))
    .agg(
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.count("*").alias("web_event_count"),
    )
    .orderBy(F.col("min_duration_sec").desc(), "country")
    .limit(5)
)

from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isin(["DE", "SG", "UK"]))
    .groupBy(F.col("s.customer_id"))
    .agg(
        F.count("*").alias("total_count"),
        F.min(F.col("we.event_time")).alias("min_event_time"),
    )
    .orderBy(F.col("total_count").desc())
)

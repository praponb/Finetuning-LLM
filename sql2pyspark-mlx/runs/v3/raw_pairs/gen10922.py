from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.duration_sec").between(285, 334))
    .groupBy(F.col("s.device"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
    .filter(F.col("pct_purchase") >= 79)
)

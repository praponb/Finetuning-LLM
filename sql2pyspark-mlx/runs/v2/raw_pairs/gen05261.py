from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.browser").isin(["Safari", "Chrome", "Firefox"]))
    .groupBy(F.col("s.customer_id"), F.col("s.device"))
    .agg(
        F.sum(F.when(F.col("we.event_type") == "purchase", F.col("we.duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
    )
    .filter(F.col("purchase_duration_sec") >= 76)
)

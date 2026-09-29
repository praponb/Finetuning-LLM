from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .groupBy(
        F.col("s.customer_id"),
        F.hour(F.col("s.started_at")).alias("started_hour"),
    )
    .agg(
        F.sum(F.when(F.col("we.page") == "/search", F.col("we.duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .filter(F.col("search_duration_sec") >= 40)
    .orderBy(F.col("customer_id").desc())
)

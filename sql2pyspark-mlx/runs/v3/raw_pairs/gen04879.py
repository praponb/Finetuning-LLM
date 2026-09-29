from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.hour(F.col("s.started_at")).between(10, 13))
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("we.event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
    .orderBy("country")
)

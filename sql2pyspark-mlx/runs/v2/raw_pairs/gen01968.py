from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.device"), F.col("s.country"))
    .agg(
        F.count(F.when(F.col("we.event_type") == "page_view", 1)).alias("page_view_count"),
    )
    .orderBy("country", F.col("page_view_count").desc())
)

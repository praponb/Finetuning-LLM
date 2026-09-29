from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.duration_sec").between(180, 260))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .orderBy("country", "id")
    .limit(10)
)

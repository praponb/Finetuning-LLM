from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser") == "Edge").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.event_type") == "click")
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("we.event_time"),
    )
    .orderBy("id")
    .limit(5)
)

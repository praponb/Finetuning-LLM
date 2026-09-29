from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("country") == "JP").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.event_type").isin(["click", "search"]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.event_type"),
        F.col("we.event_time"),
    )
)

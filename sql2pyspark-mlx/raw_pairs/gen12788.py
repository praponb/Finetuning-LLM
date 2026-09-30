from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser") != "Chrome").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.hour(F.col("we.event_time")).between(9, 17))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("we.event_time"),
    )
    .orderBy("page", "event_time")
)

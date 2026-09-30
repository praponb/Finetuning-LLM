from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_time") < "2023-07-01").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("s.device"),
    )
    .orderBy("customer_id", "id")
    .limit(10)
)

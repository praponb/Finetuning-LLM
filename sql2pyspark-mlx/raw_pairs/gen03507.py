from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("s.started_at") >= "2024-07-04") & (F.col("s.started_at") < "2023-05-06"))
    .select(
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("s.started_at").cast("date").alias("started_at_day"),
    )
    .orderBy("started_at_day")
)

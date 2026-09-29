from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("we.event_type") != "search") | (F.col("s.started_at") < "2024-02-14"))
    .select(
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.date_format(F.col("s.started_at"), "yyyy-MM").alias("started_at_label"),
    )
)

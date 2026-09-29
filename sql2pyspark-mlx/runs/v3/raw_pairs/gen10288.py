from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.to_date("started_at") >= "2024-08-15").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.event_type") == "add_to_cart")
    .select(F.col("we.id"), F.col("we.page"), F.col("we.duration_sec"))
)

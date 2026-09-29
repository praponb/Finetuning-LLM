from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .select(F.col("s.customer_id"), F.col("we.page"), F.col("we.event_type"))
)

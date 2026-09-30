from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.to_date("event_time") < "2025-06-01").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.customer_id") == 35)
    .select(F.col("s.id"), F.col("s.browser"), F.col("s.device"))
    .orderBy("browser", "device", "id")
    .limit(5)
)

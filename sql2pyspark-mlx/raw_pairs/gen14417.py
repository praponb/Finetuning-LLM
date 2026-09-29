from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.to_date("event_time") >= "2023-07-21").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.customer_id").isin([4, 26]))
    .select(F.col("s.id"), F.col("s.device"), F.col("s.browser"))
    .orderBy("device", "browser")
)

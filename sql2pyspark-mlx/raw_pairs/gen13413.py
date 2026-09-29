from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("customer_id") == 18).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.session_id").isin([3, 5, 26]))
    .select(F.col("we.id"), F.col("we.event_time"), F.col("we.page"))
)

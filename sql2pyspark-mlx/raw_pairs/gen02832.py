from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_time") < "2023-04-01").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.country"))
    .orderBy(F.col("customer_id").desc(), "country")
)

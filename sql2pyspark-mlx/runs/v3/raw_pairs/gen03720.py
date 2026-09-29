from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .select(F.col("we.page"), F.col("s.started_at"), F.col("c.name"))
)

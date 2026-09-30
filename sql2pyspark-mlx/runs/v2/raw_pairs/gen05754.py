from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .filter(F.col("s.customer_id").isNotNull())
    .join(df_customers.filter(F.col("name").like("Hen%")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .select(F.col("s.id"), F.col("s.browser"), F.col("s.started_at"))
)

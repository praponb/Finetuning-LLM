from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .filter(F.col("s.customer_id").isNotNull())
    .join(df_customers.filter(F.col("segment") == "Consumer").alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.started_at") < "2025-03-29")
    .select(F.col("s.id"), F.col("s.device"), F.col("s.started_at"))
)

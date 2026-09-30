from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .select(F.col("s.device"), F.col("c.segment"))
    .orderBy(F.col("segment").desc(), "device")
)

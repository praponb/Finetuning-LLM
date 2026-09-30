from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") == "DE")
    .select(F.col("s.device"), F.col("s.customer_id"), F.col("c.is_active"))
    .orderBy("customer_id")
)

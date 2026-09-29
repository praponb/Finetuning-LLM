from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("c.is_active"))
)

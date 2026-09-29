from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.is_active"))
    .select(F.col("s.customer_id"), F.col("s.device"), F.col("c.country"))
    .orderBy(F.col("country").desc())
)

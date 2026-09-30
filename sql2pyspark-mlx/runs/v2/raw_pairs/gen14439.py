from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(F.col("s.browser"), F.col("s.customer_id"), F.col("c.is_active"))
    .orderBy(F.col("customer_id").desc(), F.col("browser").desc())
)

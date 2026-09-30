from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") <= 50)
    .select(F.col("s.browser"), F.col("c.email"), F.col("c.city"))
)

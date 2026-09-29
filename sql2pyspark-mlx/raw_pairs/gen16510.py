from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isin([10, 27, 30]))
    .select(F.col("c.age"), F.col("c.city"), F.col("s.browser"))
)

from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["London", "Singapore"]))
    .select(F.col("s.customer_id"), F.col("c.email"), F.col("c.name"), F.col("c.age"))
)

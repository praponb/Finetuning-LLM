from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.browser").isin(["Firefox", "Edge"]) & F.col("c.age").isNotNull())
    .select(F.col("s.browser"), F.col("c.age"))
    .orderBy(F.col("age").desc())
)

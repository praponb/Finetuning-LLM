from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age").isNotNull() & (F.col("s.monthly_fee") >= 150))
    .select(F.col("s.id"), F.col("s.status"), F.col("c.is_active"))
    .orderBy(F.col("id").desc(), F.col("status").desc())
    .limit(3)
)

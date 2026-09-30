from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.plan").isin(["premium", "enterprise", "basic"]))
    .select(F.col("s.id"), F.col("s.start_date"), F.col("c.age"), F.col("c.email"))
    .orderBy(F.col("age").desc())
)

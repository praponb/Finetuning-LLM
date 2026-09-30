from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isNotNull())
    .select(F.col("c.name"), F.col("s.plan"), F.col("s.customer_id"))
    .orderBy("plan", F.col("customer_id").desc())
)

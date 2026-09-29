from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.count("*").alias("subscription_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.is_active"), F.col("c.city"), F.col("C.subscription_count"))
    .orderBy(F.col("subscription_count").desc(), "city")
)

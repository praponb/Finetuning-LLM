from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(F.col("c.email"), F.col("t.standard_count"))
    .orderBy("email", "standard_count")
)

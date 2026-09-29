from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .filter(F.col("sbc.cancelled_count") >= 73)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("sbc.cancelled_count"),
    )
    .orderBy(F.col("id").desc(), F.col("name").desc())
    .limit(5)
)

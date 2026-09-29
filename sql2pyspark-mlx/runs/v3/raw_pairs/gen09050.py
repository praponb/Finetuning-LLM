from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
    )
)

result = (
    df_customers.alias("customers")
    .join(subscriptions_by_customer.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.monthly_fee_count") > 5)
    .select(
        F.col("customers.id"),
        F.col("customers.is_active"),
        F.col("t.monthly_fee_count"),
        F.col("t.pct_basic"),
    )
    .orderBy(F.col("id").desc(), F.col("monthly_fee_count").desc())
)

from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .groupBy("customer_id")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "left")
    .filter(F.col("sbc.total_monthly_fee") >= 570)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("sbc.total_monthly_fee"),
        F.col("sbc.unique_plans"),
    )
)

from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "EXPIRED"]))
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("sbc.avg_monthly_fee"),
        F.col("sbc.paused_count"),
    )
    .orderBy(F.col("avg_monthly_fee").desc(), F.col("id").desc())
)

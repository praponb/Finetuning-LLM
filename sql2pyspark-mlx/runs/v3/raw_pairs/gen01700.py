from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.monthly_fee_count") > 8)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("ct.monthly_fee_count"),
        F.col("ct.avg_monthly_fee"),
    )
)

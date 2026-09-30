from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0))) / F.count("*")), 1).alias("pct_basic"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.credit_limit"),
        F.col("customer_summary.pct_basic"),
        F.col("customer_summary.avg_monthly_fee"),
    )
    .orderBy(F.col("pct_basic").desc())
)

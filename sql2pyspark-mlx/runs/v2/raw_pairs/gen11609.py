from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("C.avg_monthly_fee"),
        F.col("C.pct_standard"),
    )
    .orderBy(F.col("city").desc(), "id")
    .limit(10)
)

from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.avg_monthly_fee") > 79)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("cs.avg_monthly_fee"),
        F.col("cs.status_list"),
    )
    .orderBy("signup_date")
)

from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("start_date") < "2023-08-26")
    .groupBy("customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.max_monthly_fee") >= 210)
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("cs.monthly_fee_count"),
        F.col("cs.max_monthly_fee"),
    )
    .orderBy(F.col("max_monthly_fee").desc())
)

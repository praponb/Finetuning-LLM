from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("start_date") > "2025-04-26")
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.num_subscriptions") >= 7)
    .select(
        F.col("customers.id"),
        F.col("customers.country"),
        F.col("customers.name"),
        F.col("t.num_subscriptions"),
        F.col("t.avg_monthly_fee"),
    )
)

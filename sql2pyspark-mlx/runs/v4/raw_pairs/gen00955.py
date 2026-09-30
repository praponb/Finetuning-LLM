from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-07-01", "2024-12-01"))
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.min("start_date").alias("first_start_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.name"), F.col("t.avg_monthly_fee"), F.col("t.first_start_date"))
)

from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-04-02", "2025-02-02"))
    .groupBy("customer_id")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.min_monthly_fee") >= 150)
    .select(F.col("c.name"), F.col("t.min_monthly_fee"))
    .orderBy("name")
)

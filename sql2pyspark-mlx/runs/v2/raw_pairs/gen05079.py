from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("end_date") <= "2023-12-01")
    .groupBy("customer_id")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.email"), F.col("cs.monthly_fee_count"))
    .orderBy(F.col("monthly_fee_count").desc())
)

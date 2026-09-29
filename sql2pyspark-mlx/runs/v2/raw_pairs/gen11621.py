from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(F.col("c.is_active"), F.col("ct.avg_monthly_fee"))
)

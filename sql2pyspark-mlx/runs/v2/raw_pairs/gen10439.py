from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.active_monthly_fee") >= 77)
    .select(F.col("c.id"), F.col("c.email"), F.col("t.active_monthly_fee"))
)

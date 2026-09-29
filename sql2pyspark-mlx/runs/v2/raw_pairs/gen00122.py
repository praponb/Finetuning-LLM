from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .filter(F.col("C.paused_monthly_fee") > 73)
    .select(F.col("c.name"), F.col("C.paused_monthly_fee"))
    .orderBy(F.col("name").desc())
)

from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "CANCELLED", "EXPIRED"]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.is_active"),
        F.col("c.credit_limit"),
        F.col("cs.cancelled_count"),
        F.col("cs.monthly_fee_count"),
    )
)

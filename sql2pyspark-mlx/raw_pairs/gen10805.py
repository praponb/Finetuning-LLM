from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.total_monthly_fee") > 480)
    .select(F.col("c.city"), F.col("cs.pct_premium"), F.col("cs.total_monthly_fee"))
)

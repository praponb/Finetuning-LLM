from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .filter(F.col("customer_id").isin([1, 2, 40]))
    .groupBy("customer_id")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.max_monthly_fee") > 230)
    .select(
        F.col("c.segment"),
        F.col("c.country"),
        F.col("ct.max_monthly_fee"),
        F.col("ct.avg_monthly_fee"),
    )
    .orderBy(F.col("max_monthly_fee").desc())
)

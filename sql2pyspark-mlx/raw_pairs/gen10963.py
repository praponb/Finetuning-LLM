from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.min_monthly_fee") >= 190)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.segment"),
        F.col("ct.monthly_fee_range"),
        F.col("ct.min_monthly_fee"),
    )
    .orderBy(F.col("min_monthly_fee").desc(), "segment")
)

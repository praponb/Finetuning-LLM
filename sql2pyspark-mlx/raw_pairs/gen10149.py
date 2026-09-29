from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([11, 40]) & F.col("monthly_fee").isNotNull())
    .cube("customer_id", "status")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
    .orderBy(F.col("customer_id").desc())
)

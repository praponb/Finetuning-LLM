from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "standard") & F.col("customer_id").isNull())
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
)

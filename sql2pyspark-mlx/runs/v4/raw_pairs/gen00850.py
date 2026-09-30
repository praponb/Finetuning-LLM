from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([18, 31]))
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
    )
    .filter(F.col("enterprise_monthly_fee") >= 43)
    .select("plan", "enterprise_monthly_fee", "standard_monthly_fee")
    .orderBy(F.col("enterprise_monthly_fee").desc())
)

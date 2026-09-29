from pyspark.sql import functions as F

result = (
    df_subscriptions
    .cube("status", "plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
    )
    .orderBy(F.col("basic_monthly_fee").desc())
)

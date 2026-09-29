from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2025)
    .groupBy(
        F.when(F.col("monthly_fee") >= 241, "high").when(F.col("monthly_fee") >= 90, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") >= 720)
    .select("monthly_fee_bucket", "unique_customers", "total_monthly_fee")
    .orderBy("monthly_fee_bucket", "unique_customers")
)

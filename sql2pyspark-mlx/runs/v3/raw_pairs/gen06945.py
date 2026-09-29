from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") < "2023-03-18") & F.col("customer_id").isin([6, 16, 18, 33]) & F.col("monthly_fee").isNotNull())
    .groupBy("plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 110)
    .orderBy(F.col("total_monthly_fee").desc())
)

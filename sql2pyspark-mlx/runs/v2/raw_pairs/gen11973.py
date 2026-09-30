from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.month("end_date").isin([9, 10, 11]) & F.col("status").isin(["EXPIRED", "CANCELLED"])) | F.month("end_date").isin([5, 6, 11]))
    .groupBy(
        F.when(F.col("monthly_fee") >= 236, "high").when(F.col("monthly_fee") >= 43, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") < 4)
    .orderBy("monthly_fee_bucket")
)

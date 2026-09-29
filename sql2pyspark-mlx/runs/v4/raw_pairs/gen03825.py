from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([2, 8, 11]))
    .cube(
        "status",
        F.when(F.col("monthly_fee") >= 190, "high").when(F.col("monthly_fee") >= 62, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .orderBy("total_monthly_fee")
)

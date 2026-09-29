from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-09-01", "2024-03-14"))
    .groupBy(
        F.when(F.col("monthly_fee") >= 225, "high").when(F.col("monthly_fee") >= 96, "mid").otherwise("low").alias("monthly_fee_bucket"),
        F.quarter("end_date").alias("end_quarter"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
    )
    .filter(F.col("pct_premium") >= 55)
    .orderBy(F.col("end_quarter").desc())
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") < "2025-08-12") & ((F.year("start_date") == 2023) | F.month("end_date").isin([3, 7, 10])))
    .groupBy("status", "plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .filter(F.col("total_monthly_fee") < 1330)
    .drop("total_monthly_fee")
    .orderBy("plan")
)

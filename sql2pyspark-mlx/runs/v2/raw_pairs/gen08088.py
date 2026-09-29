from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "standard", "premium"]) & (F.col("end_date") <= "2024-03-01"))
    .withColumn("adjusted_monthly_fee", F.round((F.col("monthly_fee") * 1.15), 2))
)

agg = (
    filtered_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.sum("adjusted_monthly_fee").alias("total_adjusted_monthly_fee"),
        F.round(F.avg("adjusted_monthly_fee"), 2).alias("avg_adjusted_monthly_fee"),
    )
)

result = (
    agg
    .filter(F.col("total_adjusted_monthly_fee") > 210)
    .select("start_quarter", "total_adjusted_monthly_fee", "avg_adjusted_monthly_fee")
    .orderBy(F.col("total_adjusted_monthly_fee").desc())
)

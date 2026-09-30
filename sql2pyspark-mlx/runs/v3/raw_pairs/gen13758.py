from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["EXPIRED", "ACTIVE"]) & F.col("start_date").between("2023-04-02", "2025-02-02") & F.col("status").isin(["EXPIRED", "PAUSED", "ACTIVE"]))
    .groupBy("status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("enterprise_monthly_fee") >= 56)
    .orderBy(F.col("unique_statuss").desc())
)

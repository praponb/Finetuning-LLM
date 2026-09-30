from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "PAUSED"]) & (F.col("plan") == "standard") & F.col("end_date").between("2023-10-01", "2025-02-17"))
    .groupBy("status")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
    .orderBy(F.col("avg_monthly_fee").desc())
)

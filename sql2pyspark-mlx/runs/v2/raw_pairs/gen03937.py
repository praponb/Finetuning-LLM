from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 160) & F.col("plan").isin(["basic", "premium", "standard"]) & F.col("start_date").between("2024-12-01", "2025-07-01"))
    .groupBy("plan", "status", "customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee")).otherwise(0)).alias("cancelled_monthly_fee"),
    )
    .orderBy("customer_id", F.col("plan").desc())
)

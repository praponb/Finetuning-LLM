from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(100, 230) & F.col("customer_id").isin([1, 27, 37, 38]))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee")).otherwise(0)).alias("basic_monthly_fee"),
        F.max("end_date").alias("latest_end_date"),
    )
    .orderBy("latest_end_date", "status")
    .limit(5)
)

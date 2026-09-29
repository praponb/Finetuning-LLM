from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE"]) & F.col("customer_id").isNotNull())
    .groupBy("status", "plan")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.min("end_date").alias("min_end_date"),
    )
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 10, 17, 39]))
    .groupBy("status", "plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") >= 5)
    .orderBy(F.col("standard_count").desc(), "status")
)

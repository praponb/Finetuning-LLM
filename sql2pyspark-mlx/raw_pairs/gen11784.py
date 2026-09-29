from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 60) & F.col("status").isin(["ACTIVE", "CANCELLED", "PAUSED"]))
    .groupBy("customer_id", "status")
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
    .filter(F.col("standard_count") > 20)
)

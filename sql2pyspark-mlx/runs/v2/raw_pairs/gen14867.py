from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "standard") & (~F.col("status").isin(["CANCELLED", "ACTIVE"]) | (F.col("monthly_fee") >= 70)))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") > 5)
    .drop("monthly_fee_count")
)

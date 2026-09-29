from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "ACTIVE"]))
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("standard_count") >= 5)
    .orderBy("monthly_fee_count")
)

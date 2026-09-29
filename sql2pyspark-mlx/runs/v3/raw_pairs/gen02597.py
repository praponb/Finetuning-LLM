from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") & F.col("customer_id").isNull())
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
)

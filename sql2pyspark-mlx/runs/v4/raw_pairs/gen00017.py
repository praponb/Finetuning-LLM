from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.quarter("start_date").alias("start_quarter"))
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .filter(F.col("monthly_fee_range") > 69)
    .orderBy("status")
)

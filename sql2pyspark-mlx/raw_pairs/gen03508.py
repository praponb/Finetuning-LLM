from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([3, 7, 9]))
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .filter(F.col("monthly_fee_range") < 22)
)

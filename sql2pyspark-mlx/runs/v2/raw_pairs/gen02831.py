from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "CANCELLED"]))
    .groupBy("status", "customer_id")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .filter(F.col("monthly_fee_range") > 73)
    .orderBy("monthly_fee_range", "status", "customer_id")
    .limit(5)
)

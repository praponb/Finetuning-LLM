from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([9, 15, 23, 34]))
    .groupBy("plan")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .filter(F.col("monthly_fee_range") >= 23)
    .orderBy(F.col("monthly_fee_range").desc())
)

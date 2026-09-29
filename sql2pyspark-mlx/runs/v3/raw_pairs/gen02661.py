from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([4, 6, 12]) & F.col("customer_id").isNull())
    .groupBy("plan")
    .agg((F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"))
    .orderBy("monthly_fee_range")
)

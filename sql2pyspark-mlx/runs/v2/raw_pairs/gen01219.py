from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isin([23, 38]) & (F.col("end_date") > "2024-07-01"))
    .select("id", "start_date", "monthly_fee")
)

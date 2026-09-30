from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([4, 15, 24, 26]) & (F.col("customer_id") == 9))
    .select("id", "monthly_fee", "end_date", "status")
)

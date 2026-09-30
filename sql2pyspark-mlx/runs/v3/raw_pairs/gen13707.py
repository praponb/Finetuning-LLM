from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 200) & (F.col("customer_id").isNotNull() | (F.col("monthly_fee") >= 130)))
    .select("status", "customer_id")
)

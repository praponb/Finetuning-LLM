from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 210) | F.col("customer_id").isin([7, 24, 26, 30]))
    .select("id", "customer_id")
    .orderBy("id")
)

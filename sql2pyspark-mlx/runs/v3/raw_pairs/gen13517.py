from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([11, 32, 33, 40]))
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(df_store_sales.select("revenue", F.lit("store_sale").alias("source")))
)

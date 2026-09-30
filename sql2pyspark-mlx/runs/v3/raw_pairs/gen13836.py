from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(df_store_sales.select("revenue", F.lit("store_sale").alias("source")))
    .distinct()
    .orderBy("source")
)

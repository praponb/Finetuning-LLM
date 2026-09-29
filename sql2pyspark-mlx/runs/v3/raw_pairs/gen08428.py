from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .intersect(df_subscriptions.select(F.col("monthly_fee").alias("revenue")))
    .orderBy("revenue")
)

from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 6, 8]))
    .select("revenue")
    .subtract(df_orders.select(F.col("shipping_fee").alias("revenue")))
    .orderBy("revenue")
)

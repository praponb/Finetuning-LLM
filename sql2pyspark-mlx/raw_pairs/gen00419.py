from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2300)
    .select("revenue")
    .unionByName(df_orders.select(F.col("shipping_fee").alias("revenue")))
)

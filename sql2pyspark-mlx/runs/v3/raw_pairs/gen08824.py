from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("revenue").alias("value"))
    .unionByName(df_orders.select(F.col("shipping_fee").alias("value")))
)

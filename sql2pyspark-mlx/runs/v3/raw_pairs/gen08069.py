from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .unionByName(df_products.select(F.col("price").alias("unit_price")))
    .orderBy("unit_price")
)

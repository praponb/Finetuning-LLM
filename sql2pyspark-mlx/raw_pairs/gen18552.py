from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("price").alias("value"))
    .unionByName(df_order_items.select(F.col("unit_price").alias("value")))
    .orderBy(F.col("value").desc())
)

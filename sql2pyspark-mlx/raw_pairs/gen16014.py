from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 300)
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") < 8)
    .select("order_id", "unique_products")
    .orderBy(F.col("unique_products").desc())
)

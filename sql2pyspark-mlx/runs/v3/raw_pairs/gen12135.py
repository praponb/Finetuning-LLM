from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 190) & (F.col("order_id").isin([5, 13, 14]) | (F.col("order_id") == 21)))
    .groupBy("product_id")
    .agg(F.countDistinct("unit_price").alias("unique_products"))
    .orderBy("product_id")
)

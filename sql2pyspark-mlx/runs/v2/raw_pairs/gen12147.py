from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

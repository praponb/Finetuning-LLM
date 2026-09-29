from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.max("unit_price").alias("max_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") >= 370)
    .drop("min_unit_price")
    .orderBy("unique_products", "product_id")
)

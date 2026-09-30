from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .orderBy("unique_products", "product_id")
    .limit(10)
)

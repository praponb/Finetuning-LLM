from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unique_products") >= 3)
    .select("product_id", "unique_products", "unit_price_count")
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 3) & (F.col("product_id") == 35))
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy(F.col("unique_products").desc(), F.col("max_unit_price").desc())
)

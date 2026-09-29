from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 30]) | (F.col("quantity") >= 3))
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
    .orderBy(F.col("min_quantity").desc())
)

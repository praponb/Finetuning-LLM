from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & (F.col("quantity") > 4))
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .orderBy(F.col("product_id").desc())
    .limit(10)
)

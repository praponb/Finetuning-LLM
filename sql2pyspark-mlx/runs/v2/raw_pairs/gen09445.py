from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(140, 390) & F.col("unit_price").between(110, 180) & F.col("product_id").isin([19, 33, 37, 38]))
    .groupBy("order_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.countDistinct("unit_price").alias("unique_products"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") < 350)
    .drop("avg_unit_price")
    .orderBy("min_unit_price", F.col("unique_products").desc())
)

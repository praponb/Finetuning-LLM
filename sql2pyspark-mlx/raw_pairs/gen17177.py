from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isin([10, 28, 29]) | F.col("product_id").isin([11, 25, 28]))
)

order_items_stats = (
    base
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    order_items_stats
    .select("product_id", "avg_unit_price", "unit_price_count")
    .orderBy(F.col("avg_unit_price").desc())
)

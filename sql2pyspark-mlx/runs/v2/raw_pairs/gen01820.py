from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") < 5) & ((F.col("quantity") == 7) | F.col("order_id").isin([9, 33])))
    .groupBy("product_id", "order_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("order_item_count"),
    )
    .filter(F.col("avg_quantity") > 73)
    .orderBy("order_id", F.col("product_id").desc())
    .limit(50)
)

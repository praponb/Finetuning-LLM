from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("quantity").isNotNull() & F.col("product_id").isin([1, 18, 24, 40]))
)

order_items_stats = (
    src
    .groupBy("order_id", "product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    order_items_stats
    .select("order_id", "product_id", "quantity_range", "avg_unit_price")
    .orderBy(F.col("order_id").desc())
)

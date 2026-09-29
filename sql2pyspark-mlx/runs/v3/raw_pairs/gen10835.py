from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(~(F.col("quantity").between(5, 6) & F.col("product_id").isNotNull() & (F.col("quantity") != 8)))
    .withColumn("unit_price_rounded", F.round("unit_price", 2))
)

order_items_stats = (
    recent_order_items
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price_rounded"), 2).alias("stddev_unit_price_rounded"))
)

result = (
    order_items_stats
    .select("product_id", "stddev_unit_price_rounded")
    .orderBy(F.col("stddev_unit_price_rounded").desc())
)

from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter((F.col("product_id") == 33) & F.col("order_id").isin([1, 12, 18, 37]))
)

order_items_stats = (
    recent_order_items
    .groupBy("product_id", "order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    order_items_stats
    .select("product_id", "order_id")
    .orderBy("product_id", F.col("order_id").desc())
)

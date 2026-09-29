from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter((F.col("unit_price") < 220) & (F.col("product_id") == 34) & F.col("quantity").isNull())
)

order_items_stats = (
    valid_order_items
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
)

result = (
    order_items_stats
    .filter(F.col("quantity_range") >= 62)
    .select("product_id", "quantity_range")
    .orderBy(F.col("product_id").desc())
)

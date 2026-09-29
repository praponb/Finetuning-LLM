from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 280) & (F.col("quantity") == 8))
    .cube("order_id", "product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .orderBy(F.col("order_id").desc())
)

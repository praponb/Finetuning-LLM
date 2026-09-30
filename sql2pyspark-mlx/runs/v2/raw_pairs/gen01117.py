from pyspark.sql import functions as F

result = (
    df_order_items
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .orderBy(F.col("unit_price_range").desc())
)

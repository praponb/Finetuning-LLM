from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("product_id").isin([9, 20, 28, 34]))

agg = (
    recent_order_items
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = agg.filter(F.col("unit_price_range") > 22)

from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .filter(F.col("unit_price_range") > 36)
    .orderBy("product_id")
    .limit(5)
)

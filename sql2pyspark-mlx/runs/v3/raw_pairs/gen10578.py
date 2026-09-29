from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 6) & (F.col("unit_price") >= 310))
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .orderBy("product_id")
    .limit(3)
)

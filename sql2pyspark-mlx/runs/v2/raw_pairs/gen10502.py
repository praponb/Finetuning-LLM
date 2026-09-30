from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 200) & F.col("order_id").isin([16, 17, 37, 38]))
    .groupBy("order_id", "product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
    .orderBy(F.col("unit_price_range").desc(), "order_id", "product_id")
    .limit(3)
)

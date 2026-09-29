from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("unit_price") > 270)
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)

summary = (
    valid_order_items
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    summary
    .filter(F.col("unit_price_range") >= 67)
    .select("product_id", "unit_price_range")
    .orderBy(F.col("unit_price_range").desc(), "product_id")
    .limit(50)
)

from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([1, 27, 31, 40]))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

summary = (
    valid_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.max("line_total").alias("max_line_total"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = summary

from pyspark.sql import functions as F

base = (
    df_order_items
    .filter((F.col("quantity") <= 8) & (F.col("order_id").isin([11, 17, 25, 29]) | F.col("order_id").isin([25, 40])))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

agg = (
    base
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("line_total"), 2).alias("stddev_line_total"),
    )
)

result = (
    agg
    .select("product_id", "row_count", "stddev_line_total")
    .orderBy("product_id", "stddev_line_total")
    .limit(3)
)

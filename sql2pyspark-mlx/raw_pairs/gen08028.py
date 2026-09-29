from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter((F.col("quantity") < 7) & (F.col("quantity") != 7))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

order_items_stats = (
    recent_order_items
    .groupBy("order_id")
    .agg(
        (F.max("line_total") - F.min("line_total")).alias("line_total_range"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = order_items_stats.filter(F.col("avg_unit_price") >= 72)

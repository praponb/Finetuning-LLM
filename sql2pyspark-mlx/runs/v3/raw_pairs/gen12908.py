from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() | (F.col("quantity") < 6))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("order_item_count"),
    )
    .filter(F.col("order_item_count") < 2)
)

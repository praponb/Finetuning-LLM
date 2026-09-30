from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("order_item_count"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .orderBy(F.col("order_item_count").desc())
)

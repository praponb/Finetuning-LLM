from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 8) | F.col("order_id").isin([2, 8]))
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

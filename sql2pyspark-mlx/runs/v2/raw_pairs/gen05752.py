from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 9, 12, 15]) | (F.col("quantity") < 7))
    .groupBy("order_id", "product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

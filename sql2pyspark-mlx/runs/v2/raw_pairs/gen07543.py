from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 6)
    .groupBy("order_id", "product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

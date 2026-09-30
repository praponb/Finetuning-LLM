from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("order_id").isin([20, 23]) & F.col("product_id").isin([23, 37]))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
)

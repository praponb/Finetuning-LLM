from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 8))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("avg_unit_price") > 58)
)

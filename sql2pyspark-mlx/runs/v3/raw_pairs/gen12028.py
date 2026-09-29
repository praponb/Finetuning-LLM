from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
    )
)

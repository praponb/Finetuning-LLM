from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)

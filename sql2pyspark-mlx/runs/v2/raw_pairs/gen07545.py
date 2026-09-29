from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 140)
    .groupBy("order_id", "product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)

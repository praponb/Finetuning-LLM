from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 140)
    .groupBy("product_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)

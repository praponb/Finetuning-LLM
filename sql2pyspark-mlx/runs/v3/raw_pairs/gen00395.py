from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 33, 36, 38]) & F.col("product_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
)

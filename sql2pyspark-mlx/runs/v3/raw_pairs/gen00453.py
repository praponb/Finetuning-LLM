from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 18) & (F.col("unit_price") <= 320))
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("total_unit_price") >= 2300)
    .orderBy(F.col("max_quantity").desc())
)

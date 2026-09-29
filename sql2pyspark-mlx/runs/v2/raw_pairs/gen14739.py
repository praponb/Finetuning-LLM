from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("total_quantity") < 24)
    .orderBy(F.col("avg_unit_price").desc(), "product_id")
    .limit(10)
)

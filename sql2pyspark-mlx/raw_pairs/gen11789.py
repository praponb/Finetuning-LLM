from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 54)
    .orderBy("avg_unit_price")
)

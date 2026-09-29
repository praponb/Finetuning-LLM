from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") < 140)
    .orderBy("avg_unit_price", "product_id")
    .limit(20)
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(210, 230))
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(200, 230) & F.col("product_id").isNull() & (F.col("quantity") != 4))
    .groupBy("product_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

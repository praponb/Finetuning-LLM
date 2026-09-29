from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull() | (F.col("product_id") == 40))
    .groupBy("product_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc())
)

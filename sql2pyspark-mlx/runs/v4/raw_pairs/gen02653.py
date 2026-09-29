from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 13)
    .groupBy("product_id", "order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
    .orderBy(F.col("product_id").desc())
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
    .filter(F.col("min_unit_price") > 180)
    .orderBy("min_unit_price", "product_id")
    .limit(1)
)

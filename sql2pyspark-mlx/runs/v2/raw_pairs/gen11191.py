from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .orderBy(F.col("order_id").desc())
)

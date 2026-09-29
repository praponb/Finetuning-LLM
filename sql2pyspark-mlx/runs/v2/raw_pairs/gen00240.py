from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .orderBy(F.col("total_quantity").desc(), F.col("product_id").desc())
)

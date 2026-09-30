from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") >= 4.7)
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 5)
    .groupBy("order_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") > 6.6)
)

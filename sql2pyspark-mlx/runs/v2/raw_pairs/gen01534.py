from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") > 5.3)
    .select("product_id", "min_quantity")
)

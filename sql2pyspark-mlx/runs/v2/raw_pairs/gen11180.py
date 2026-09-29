from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([10, 24, 37, 40]) & (F.col("quantity") < 7))
    .groupBy("product_id", "order_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") < 2)
    .orderBy(F.col("order_id").desc())
)

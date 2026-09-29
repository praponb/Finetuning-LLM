from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(F.count("*").alias("total_count"))
    .orderBy("product_id")
)

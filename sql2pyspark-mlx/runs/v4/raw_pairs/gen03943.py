from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") == 8)
    .groupBy("order_id")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 9)
    .select("order_id", "total_count")
)

from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("*").alias("order_item_count"))
    .orderBy("order_id")
    .limit(1)
)

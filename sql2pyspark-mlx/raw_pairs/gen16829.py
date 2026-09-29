from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy("quantity_count")
)

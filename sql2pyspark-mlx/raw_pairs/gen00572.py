from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("product_id").isNull())

product_id_summary = (
    filtered_order_items
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = product_id_summary.filter(F.col("total_quantity") >= 35)

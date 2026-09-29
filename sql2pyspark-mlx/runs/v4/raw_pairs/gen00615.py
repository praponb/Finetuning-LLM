from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("product_id").isin([1, 9, 15, 40]))

product_id_summary = (
    filtered_order_items
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("cnt"),
    )
)

result = (
    product_id_summary
    .filter(F.col("total_quantity") >= 38)
    .select("product_id", "total_quantity", "cnt")
)

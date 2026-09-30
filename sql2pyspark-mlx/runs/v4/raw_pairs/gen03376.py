from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("product_id") == 20)
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.total_quantity") > 28)
    .select(F.col("o.discount"), F.col("os.total_quantity"))
)

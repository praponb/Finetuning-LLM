from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.min_quantity") >= 3.9)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("os.min_quantity"),
    )
)

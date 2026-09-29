from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("os.total_unit_price"),
        F.col("os.unique_products"),
    )
    .orderBy("id", "order_date")
)

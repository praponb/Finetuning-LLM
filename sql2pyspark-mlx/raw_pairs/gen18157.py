from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.max_quantity") >= 3.3)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("O.max_quantity"),
    )
    .orderBy("discount")
)

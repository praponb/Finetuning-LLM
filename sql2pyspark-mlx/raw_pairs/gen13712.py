from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("order_id").isin([6, 10, 30]))
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("t.revenue"),
        F.col("t.order_item_count"),
    )
    .orderBy("order_item_count")
)

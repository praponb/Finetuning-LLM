from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.product_name").like("Ba%"))
    .groupBy(F.col("o.channel"))
    .agg(
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("channel").desc())
)

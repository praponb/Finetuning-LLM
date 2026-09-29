from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        F.max(F.col("oi.quantity")).alias("max_quantity"),
    )
    .orderBy("max_quantity")
)

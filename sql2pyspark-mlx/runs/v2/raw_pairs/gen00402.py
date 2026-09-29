from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("o.status").isin(["PENDING", "COMPLETED", "RETURNED"]))
    .groupBy(F.col("o.channel"))
    .agg(
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
    )
    .filter(F.col("revenue") > 21)
)

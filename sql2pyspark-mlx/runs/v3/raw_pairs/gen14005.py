from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
    .orderBy("avg_quantity")
)

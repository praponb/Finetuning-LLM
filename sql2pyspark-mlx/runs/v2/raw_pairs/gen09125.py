from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isin([9, 30]))
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .filter(F.col("avg_unit_price") >= 23)
)

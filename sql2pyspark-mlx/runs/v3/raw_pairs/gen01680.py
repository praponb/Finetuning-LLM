from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter(F.col("o.shipping_fee") <= 24)
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
    )
)

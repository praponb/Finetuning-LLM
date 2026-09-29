from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.product_id").isin([23, 28, 29]) | F.col("o.discount").between(0.35, 0.36))
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("p.stock_qty"),
        F.col("o.store_id"),
        F.col("o.order_date"),
    )
    .orderBy("unit_price")
)

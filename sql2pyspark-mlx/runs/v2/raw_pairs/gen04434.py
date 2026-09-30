from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("o.amount"),
    )
    .orderBy(F.col("order_id").desc(), "stock_qty")
)

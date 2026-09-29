from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("p.stock_qty").isNull() | F.col("oi.order_id").isin([13, 37]))
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("p.supplier_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("discount").desc(), F.col("supplier_id").desc())
)

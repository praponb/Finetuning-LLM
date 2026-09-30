from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(~((F.col("oi.order_id") == 16) & (F.col("oi.quantity") <= 3)))
    .select(
        F.col("oi.quantity"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("o.discount"),
        F.least(F.col("oi.quantity"), F.col("oi.unit_price")).alias("least_quantity_unit_price"),
    )
    .orderBy(F.col("price").desc())
)

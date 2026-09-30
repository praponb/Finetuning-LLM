from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.quantity") > 5)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
)

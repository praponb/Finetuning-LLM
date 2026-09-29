from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("p.stock_qty").between(120, 254))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
)

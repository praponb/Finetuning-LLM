from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.product_id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("product_id").desc(), F.col("stock_qty").desc())
)

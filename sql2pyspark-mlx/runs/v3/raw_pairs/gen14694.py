from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .select(
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
    )
)

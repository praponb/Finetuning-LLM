from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
)

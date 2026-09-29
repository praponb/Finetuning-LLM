from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .select(
        F.col("p.price"),
        F.col("p.brand"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        (F.col("oi.unit_price") * F.col("oi.quantity")).alias("line_total"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.product_id").isin([7, 37]) & (F.col("p.category") != "Home"))
    .select(
        F.col("p.product_name"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
    .orderBy(F.col("unit_price").desc())
)

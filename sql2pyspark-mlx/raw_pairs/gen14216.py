from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.order_id").isin([4, 18, 38]) & (F.col("oi.quantity") < 5))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("p.category"),
        F.col("p.brand"),
    )
)

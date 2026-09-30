from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.discount"),
    )
    .orderBy(F.col("product_id").desc())
)

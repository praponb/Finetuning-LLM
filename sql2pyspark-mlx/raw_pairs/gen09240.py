from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.stock_qty") <= 118)
    .select(
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("p.brand"),
    )
    .orderBy("cost")
)

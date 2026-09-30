from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.filter(F.col("stock_qty") == 306).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_semi")
    .filter(F.col("oi.quantity") <= 7)
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
    )
    .orderBy(F.col("order_id").desc())
)

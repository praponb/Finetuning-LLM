from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty") <= 124)
    .select(
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.supplier_id"),
        F.col("oi.unit_price").cast("int").alias("unit_price_int"),
    )
    .orderBy("supplier_id")
)

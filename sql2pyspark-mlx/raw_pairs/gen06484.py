from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.order_id").isin([9, 18, 21, 29]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.coalesce(F.col("oi.unit_price"), F.lit(0)).alias("unit_price_filled"),
    )
)

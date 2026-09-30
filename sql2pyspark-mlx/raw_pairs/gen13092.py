from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.max_unit_price") > 130)
    .select(
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("ps.max_unit_price"),
        F.col("ps.quantity_count"),
    )
    .orderBy(F.col("max_unit_price").desc())
)

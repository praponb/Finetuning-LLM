from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.max_unit_price") >= 210)
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("ps.max_unit_price"),
        F.col("ps.min_quantity"),
    )
    .orderBy(F.col("id").desc(), F.col("max_unit_price").desc())
    .limit(10)
)

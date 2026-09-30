from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_quantity") >= 80)
    .select(
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("t.unit_price_count"),
        F.col("t.avg_quantity"),
    )
)

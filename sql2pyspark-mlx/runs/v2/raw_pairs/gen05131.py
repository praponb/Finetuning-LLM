from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.quantity_range") > 62)
    .select(
        F.col("p.stock_qty"),
        F.col("ps.stddev_quantity"),
        F.col("ps.quantity_range"),
    )
)

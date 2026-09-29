from pyspark.sql import functions as F

product_stats = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("ps.total_quantity"),
        F.col("ps.stddev_quantity"),
    )
)

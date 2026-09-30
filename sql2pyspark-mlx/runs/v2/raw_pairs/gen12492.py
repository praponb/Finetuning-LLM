from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("product_id").isin([4, 8, 10]))
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("t.total_quantity"),
        F.col("t.stddev_quantity"),
    )
)

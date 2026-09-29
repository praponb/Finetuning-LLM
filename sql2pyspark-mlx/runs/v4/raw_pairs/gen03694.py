from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("product_id") == 4)
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("ps.quantity_range"),
        F.col("ps.stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), F.col("quantity_range").desc())
)

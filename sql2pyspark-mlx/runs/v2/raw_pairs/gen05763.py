from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("quantity").between(134, 210))
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("t.quantity_count"),
        F.col("t.stddev_quantity"),
    )
)

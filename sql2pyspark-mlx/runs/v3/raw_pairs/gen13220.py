from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.stddev_quantity") > 45)
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("ps.total_count"),
        F.col("ps.stddev_quantity"),
    )
)

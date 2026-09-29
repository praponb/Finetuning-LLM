from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("ps.avg_quantity"),
        F.col("ps.max_quantity"),
    )
)

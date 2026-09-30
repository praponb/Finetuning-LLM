from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(F.col("p.launch_date"), F.col("ps.min_quantity"), F.col("ps.max_quantity"))
)

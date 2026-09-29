from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.avg_quantity") > 170)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("ps.avg_quantity"),
        F.col("ps.row_count"),
    )
    .orderBy(F.col("id").desc(), F.col("avg_quantity").desc())
)

from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.quantity_count") > 4)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("ps.total_quantity"),
        F.col("ps.quantity_count"),
    )
)

from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .filter(F.col("ps.min_quantity") > 210)
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("ps.min_quantity"),
        F.col("ps.first_last_restock"),
    )
    .orderBy("launch_date")
)

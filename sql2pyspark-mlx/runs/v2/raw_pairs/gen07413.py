from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("quantity") > 201)
    .groupBy("product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("ps.latest_last_restock"),
        F.col("ps.unique_stores"),
    )
    .orderBy("id", "launch_date")
)

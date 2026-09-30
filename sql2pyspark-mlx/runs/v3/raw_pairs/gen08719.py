from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.unique_products") >= 9)
    .select(
        F.col("p.launch_date"),
        F.col("ps.unique_products"),
        F.col("ps.quantity_range"),
    )
    .orderBy("launch_date")
)

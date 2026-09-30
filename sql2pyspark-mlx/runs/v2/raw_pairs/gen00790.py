from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.quantity_range") >= 69)
    .select(F.col("p.launch_date"), F.col("ps.row_count"), F.col("ps.quantity_range"))
    .orderBy(F.col("launch_date").desc())
)

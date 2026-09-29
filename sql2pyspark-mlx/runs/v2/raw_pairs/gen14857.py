from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.quantity_range") > 53)
    .select(F.col("p.supplier_id"), F.col("ps.quantity_range"), F.col("ps.row_count"))
)

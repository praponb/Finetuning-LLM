from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("launch_date").between("2024-03-19", "2024-09-18"))
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0)).alias("stark_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "left")
    .filter(F.col("supplier_summary.price_range") >= 77)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("supplier_summary.price_range"),
        F.col("supplier_summary.stark_count"),
    )
)

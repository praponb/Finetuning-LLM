from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("first_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("S.first_launch_date"),
    )
)

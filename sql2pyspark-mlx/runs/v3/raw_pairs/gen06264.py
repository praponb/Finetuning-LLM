from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
        F.col("supplier_totals.avg_cost"),
        F.col("supplier_totals.row_count"),
    )
    .orderBy(F.col("row_count").desc())
)

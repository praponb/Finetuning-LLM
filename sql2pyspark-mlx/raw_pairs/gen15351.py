from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("first_launch_date"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("SupplierSummary"), F.col("suppliers.id") == F.col("SupplierSummary.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.country"),
        F.col("SupplierSummary.first_launch_date"),
    )
    .orderBy("supplier_name")
)

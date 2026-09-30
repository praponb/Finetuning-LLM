from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("cost").isNotNull())
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("last_launch_date"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("supplier_summary.last_launch_date"),
    )
)

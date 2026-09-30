from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("supplier_id") == 20)
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("latest_launch_date"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("SupplierStats"), F.col("suppliers.id") == F.col("SupplierStats.supplier_id"), "inner")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("suppliers.supplier_name"),
        F.col("SupplierStats.latest_launch_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)

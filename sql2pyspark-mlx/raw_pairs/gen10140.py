from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Umbrella"]))
    .groupBy("supplier_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("SupplierStats"), F.col("suppliers.id") == F.col("SupplierStats.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.supplier_name"),
        F.col("SupplierStats.cnt"),
    )
)

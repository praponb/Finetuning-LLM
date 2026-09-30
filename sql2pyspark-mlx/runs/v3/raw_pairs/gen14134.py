from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("SupplierStats"), F.col("suppliers.id") == F.col("SupplierStats.supplier_id"), "left")
    .select(
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        F.col("SupplierStats.brand_list"),
    )
)

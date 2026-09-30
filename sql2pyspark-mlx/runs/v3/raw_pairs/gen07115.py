from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("SupplierTotals"), F.col("suppliers.id") == F.col("SupplierTotals.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("SupplierTotals.category_values"),
        F.col("SupplierTotals.avg_price"),
    )
    .orderBy("supplier_name", "category_values")
)

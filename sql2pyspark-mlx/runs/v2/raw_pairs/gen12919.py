from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(~F.col("product_name").like("%Bottl%"))
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_count"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("SupplierStats"), F.col("suppliers.id") == F.col("SupplierStats.supplier_id"), "inner")
    .filter(F.col("SupplierStats.pct_globex") >= 57)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("SupplierStats.grocery_count"),
        F.col("SupplierStats.pct_globex"),
    )
)

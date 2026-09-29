from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("ProductsBySupplier"), F.col("suppliers.id") == F.col("ProductsBySupplier.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("ProductsBySupplier.fashion_count"),
    )
    .orderBy("city", "id")
)

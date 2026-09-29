from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("ProductsBySupplier"), F.col("suppliers.id") == F.col("ProductsBySupplier.supplier_id"), "inner")
    .select(F.col("suppliers.city"), F.col("ProductsBySupplier.brand_list"))
)

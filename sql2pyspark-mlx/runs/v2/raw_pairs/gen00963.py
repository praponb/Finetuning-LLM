from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .filter(F.col("products_by_supplier.initech_count") > 70)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.supplier_name"),
        F.col("products_by_supplier.initech_count"),
        F.col("products_by_supplier.acme_count"),
    )
    .orderBy("initech_count")
)

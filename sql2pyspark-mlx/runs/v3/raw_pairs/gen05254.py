from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("suppliers.supplier_name"), F.col("t.globex_count"))
)

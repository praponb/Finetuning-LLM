from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.size("tags") > 2)
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.rating"), F.col("s.supplier_name"), F.col("pbs.globex_count"))
    .orderBy(F.col("supplier_name").desc())
)

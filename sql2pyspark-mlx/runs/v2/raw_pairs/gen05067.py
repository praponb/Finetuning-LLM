from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.total_cost") >= 1500)
    .select(F.col("s.id"), F.col("s.rating"), F.col("pbs.total_cost"))
)

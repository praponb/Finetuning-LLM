from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty").between(148, 213))
    .groupBy("supplier_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.total_cost") > 2800)
    .select(F.col("s.supplier_name"), F.col("t.total_cost"))
    .orderBy(F.col("total_cost").desc())
)

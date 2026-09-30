from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost").isNotNull())
    .groupBy("supplier_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.supplier_name"), F.col("t.avg_cost"))
)

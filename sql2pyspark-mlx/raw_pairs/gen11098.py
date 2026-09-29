from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty") != 382)
    .groupBy("supplier_id")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .filter(F.col("pbs.avg_cost") > 450)
    .select(F.col("s.supplier_name"), F.col("s.rating"), F.col("pbs.avg_cost"))
)

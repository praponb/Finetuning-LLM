from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty").between(200, 206))
    .groupBy("supplier_id")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.country"), F.col("pbs.total_stock_qty"))
    .orderBy(F.col("id").desc())
)

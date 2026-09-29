from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("supplier_id") == 4)
    .groupBy("supplier_id")
    .agg(F.max("stock_qty").alias("max_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("s.country"), F.col("pbs.max_stock_qty"))
)

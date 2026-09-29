from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.avg_stock_qty") > 390)
    .select(F.col("s.city"), F.col("pbs.avg_stock_qty"))
    .orderBy("avg_stock_qty")
)

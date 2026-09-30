from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("SupplierTotals"), F.col("suppliers.id") == F.col("SupplierTotals.supplier_id"), "left")
    .filter(F.col("SupplierTotals.avg_stock_qty") > 24)
    .select(F.col("suppliers.country"), F.col("SupplierTotals.avg_stock_qty"))
)

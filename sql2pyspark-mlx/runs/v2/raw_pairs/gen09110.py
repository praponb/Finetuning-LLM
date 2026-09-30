from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_totals.alias("supplier_totals"), F.col("suppliers.id") == F.col("supplier_totals.supplier_id"), "inner")
    .select(F.col("suppliers.city"), F.col("supplier_totals.total_stock_qty"))
    .orderBy("city", F.col("total_stock_qty").desc())
)

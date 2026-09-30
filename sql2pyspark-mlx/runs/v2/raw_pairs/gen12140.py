from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.min_stock_qty") > 200)
    .select(F.col("s.city"), F.col("s.supplier_name"), F.col("ss.min_stock_qty"))
    .orderBy("supplier_name")
)

from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stock_qty_count") >= 5)
    .select(F.col("s.city"), F.col("s.supplier_name"), F.col("t.stock_qty_count"))
)

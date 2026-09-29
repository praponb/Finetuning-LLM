from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.size("tags") >= 2)
    .groupBy("supplier_id")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.avg_stock_qty") > 44)
    .select(F.col("s.city"), F.col("s.supplier_name"), F.col("ss.avg_stock_qty"))
    .orderBy("city", F.col("avg_stock_qty").desc())
)

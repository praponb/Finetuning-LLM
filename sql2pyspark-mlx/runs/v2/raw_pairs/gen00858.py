from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.countDistinct("stock_qty").alias("unique_suppliers"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("ss.unique_suppliers"))
)

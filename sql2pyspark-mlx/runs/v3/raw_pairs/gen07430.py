from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg((F.max("price") - F.min("price")).alias("price_range"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("s.supplier_name"), F.col("ss.price_range"))
)

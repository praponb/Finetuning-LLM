from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.fashion_count") >= 4)
    .select(F.col("s.city"), F.col("ss.fashion_count"))
)

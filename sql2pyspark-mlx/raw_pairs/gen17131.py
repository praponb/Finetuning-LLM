from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.fashion_count") >= 7)
    .select(F.col("s.supplier_name"), F.col("s.city"), F.col("t.fashion_count"))
)

from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stark_count") >= 9)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.rating"), F.col("t.stark_count"))
)

from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .groupBy("supplier_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .filter(F.col("S.cnt") >= 5)
    .select(F.col("s.id"), F.col("s.country"), F.col("S.cnt"))
)

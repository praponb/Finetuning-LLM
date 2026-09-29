from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.cnt") >= 8)
    .select(F.col("s.id"), F.col("s.country"), F.col("s.rating"), F.col("ss.cnt"))
)

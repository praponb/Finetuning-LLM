from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"), F.col("ss.home_count"))
    .orderBy("country")
)

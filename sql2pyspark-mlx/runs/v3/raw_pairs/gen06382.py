from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost") >= 590)
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("ss.total_count"), F.col("ss.sports_count"))
)

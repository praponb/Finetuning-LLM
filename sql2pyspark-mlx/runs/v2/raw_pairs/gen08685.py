from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("supplier_id") == 18)
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.electronics_count") >= 5)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("t.last_launch_date"),
        F.col("t.electronics_count"),
    )
)

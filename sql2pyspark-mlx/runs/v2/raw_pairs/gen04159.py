from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.avg_cost") > 280)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("t.min_launch_date"),
        F.col("t.avg_cost"),
    )
    .orderBy("min_launch_date", "id")
    .limit(20)
)

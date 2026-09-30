from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .groupBy("supplier_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.max_cost") > 280)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("t.avg_cost"),
        F.col("t.max_cost"),
    )
    .orderBy(F.col("avg_cost").desc())
)

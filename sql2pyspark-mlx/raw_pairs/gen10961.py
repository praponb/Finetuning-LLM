from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("t.unique_brands"),
        F.col("t.avg_cost"),
    )
)

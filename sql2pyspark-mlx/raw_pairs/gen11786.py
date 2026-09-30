from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.total_cost") > 1300)
    .select(F.col("s.city"), F.col("S.total_cost"), F.col("S.cost_count"))
    .orderBy("cost_count")
)

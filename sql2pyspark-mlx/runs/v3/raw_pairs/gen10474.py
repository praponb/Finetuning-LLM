from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.cost_count") > 8)
    .select(F.col("s.rating"), F.col("S.last_launch_date"), F.col("S.cost_count"))
    .orderBy(F.col("rating").desc())
)

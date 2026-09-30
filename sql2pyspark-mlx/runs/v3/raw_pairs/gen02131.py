from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.month("launch_date").isin([9, 10, 11]))
    .groupBy("supplier_id")
    .agg(F.avg("price").alias("avg_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.avg_price") >= 450)
    .select(F.col("s.id"), F.col("s.country"), F.col("t.avg_price"))
    .orderBy(F.col("avg_price").desc(), "id")
    .limit(5)
)

from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.avg_cost") > 200)
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"), F.col("t.avg_cost"))
    .orderBy(F.col("avg_cost").desc())
)

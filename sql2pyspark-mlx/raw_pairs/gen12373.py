from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.avg_cost") > 52)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("S.avg_cost"))
    .orderBy(F.col("id").desc(), "avg_cost")
    .limit(50)
)

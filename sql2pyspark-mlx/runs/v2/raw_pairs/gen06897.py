from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.cost_count") > 3)
    .select(F.col("s.id"), F.col("s.country"), F.col("t.cost_count"))
    .orderBy("country")
)

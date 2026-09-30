from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("supplier_id").isin([5, 24, 35, 40]))
    .groupBy("supplier_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.rating"), F.col("t.max_cost"))
    .orderBy("id", F.col("rating").desc())
    .limit(10)
)

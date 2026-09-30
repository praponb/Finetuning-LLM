from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.initech_count") >= 5)
    .select(F.col("s.id"), F.col("s.rating"), F.col("t.initech_count"))
    .orderBy(F.col("rating").desc())
)

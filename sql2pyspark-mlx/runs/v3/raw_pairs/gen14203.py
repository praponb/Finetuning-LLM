from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg((F.max("price") - F.min("price")).alias("price_range"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.price_range") > 61)
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"), F.col("t.price_range"))
    .orderBy(F.col("city").desc())
)

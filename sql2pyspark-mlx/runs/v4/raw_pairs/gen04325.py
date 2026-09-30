from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.array_contains("tags", "eco")).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.city").isin(["Chiang Mai", "Singapore", "New York"]))
    .select(F.col("s.id"), F.col("s.country"))
)

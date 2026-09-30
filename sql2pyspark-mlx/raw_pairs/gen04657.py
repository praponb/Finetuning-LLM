from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("category") == "Home").alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.city").isin(["Bangkok", "Tokyo", "Singapore"]))
    .select(F.col("s.id"), F.col("s.country"))
    .orderBy(F.col("country").desc())
)

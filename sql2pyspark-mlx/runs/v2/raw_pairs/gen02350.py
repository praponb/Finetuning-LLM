from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.city").isin(["Bangkok", "Chiang Mai", "New York"]))
    .select(F.col("s.id"), F.col("s.city"))
    .orderBy(F.col("id").desc(), "city")
)

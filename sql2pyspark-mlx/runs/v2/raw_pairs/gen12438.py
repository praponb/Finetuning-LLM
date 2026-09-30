from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.city").isin(["London", "Singapore", "Chiang Mai"]))
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("s.rating"), F.col("s.city"))
)

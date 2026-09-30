from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.array_contains("tags", "sale")).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.country") == "UK")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.rating"), F.col("s.supplier_name"))
)

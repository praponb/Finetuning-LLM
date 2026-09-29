from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.country") == "UK")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("s.country"))
)

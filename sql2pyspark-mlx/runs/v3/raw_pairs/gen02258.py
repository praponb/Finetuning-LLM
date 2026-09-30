from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.brand") == "Umbrella")
    .select(F.col("s.supplier_name"), F.col("s.country"), F.col("p.supplier_id"))
)

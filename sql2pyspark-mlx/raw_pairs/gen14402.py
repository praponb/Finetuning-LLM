from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.brand") == "Acme")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("s.country"))
    .orderBy("country", "id")
    .limit(10)
)

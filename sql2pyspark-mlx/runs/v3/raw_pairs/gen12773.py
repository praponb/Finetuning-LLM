from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.city") == "Berlin")
    .select(F.col("p.id"), F.col("p.price"), F.col("s.country"))
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(F.col("p.price"), F.col("p.cost"), F.col("s.country"))
    .orderBy(F.col("country").desc())
)

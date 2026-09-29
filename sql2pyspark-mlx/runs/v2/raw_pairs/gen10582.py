from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("rating") < 3.8).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.supplier_id"))
    .orderBy(F.col("id").desc())
)

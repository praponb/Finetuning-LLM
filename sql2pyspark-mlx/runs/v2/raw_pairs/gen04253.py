from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id") == 16).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating") > 3.3)
    .select(F.col("s.id"), F.col("s.rating"), F.col("s.city"), F.col("s.country"))
    .orderBy(F.col("rating").desc(), "country", "id")
    .limit(5)
)

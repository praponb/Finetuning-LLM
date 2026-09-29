from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.size("tags") >= 2).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating").isNotNull())
    .select(F.col("s.id"), F.col("s.rating"))
    .orderBy("id", "rating")
)

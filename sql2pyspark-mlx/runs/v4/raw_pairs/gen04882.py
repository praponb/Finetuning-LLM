from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.upper(F.trim("product_name")) == "YOGA MAT").alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.rating"))
    .orderBy(F.col("id").desc(), "rating")
    .limit(5)
)

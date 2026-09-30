from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.year("launch_date") >= 2024).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.country"))
    .orderBy(F.col("id").desc())
    .limit(50)
)

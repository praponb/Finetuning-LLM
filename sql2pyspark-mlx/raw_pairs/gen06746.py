from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
    .orderBy(F.col("country").desc())
)

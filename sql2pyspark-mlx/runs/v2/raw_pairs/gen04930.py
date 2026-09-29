from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id").isNotNull()).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating").isNotNull())
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)

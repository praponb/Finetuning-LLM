from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("supplier_id").isin([7, 20, 33])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)

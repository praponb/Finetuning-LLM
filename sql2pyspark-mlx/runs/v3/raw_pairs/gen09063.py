from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("cost") <= 360).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating").between(1.8, 3.1))
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").between(2.2, 3.6))
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
)

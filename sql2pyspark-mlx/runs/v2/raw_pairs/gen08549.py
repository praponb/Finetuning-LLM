from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy("country", F.col("id").desc())
)

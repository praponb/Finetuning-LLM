from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("s.rating"),
    )
)

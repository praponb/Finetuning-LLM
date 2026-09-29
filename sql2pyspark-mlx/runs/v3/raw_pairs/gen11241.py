from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.price") > 810)
    .select(
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
)

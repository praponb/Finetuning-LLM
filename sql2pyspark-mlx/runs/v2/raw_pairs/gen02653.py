from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.upper(F.col("s.supplier_name")).like("%OKY%"))
    .select(
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("s.rating"),
        F.col("s.country"),
    )
    .orderBy(F.col("cost").desc(), "product_name")
)

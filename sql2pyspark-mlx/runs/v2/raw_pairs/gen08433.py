from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.rating") > 2.4)
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
        F.col("p.category"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("rating").desc(), "product_name")
)

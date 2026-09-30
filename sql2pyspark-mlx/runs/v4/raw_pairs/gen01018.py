from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "London")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.supplier_name"),
        F.upper(F.col("p.product_name")).alias("product_name_upper"),
    )
    .orderBy("category", F.col("supplier_name").desc(), "id")
    .limit(50)
)

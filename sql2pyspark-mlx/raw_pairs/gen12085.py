from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.category").isin(["Grocery", "Sports", "Books"]))
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.supplier_name"),
        F.concat(F.col("s.supplier_name"), F.lit(" - "), F.col("s.city")).alias("supplier_name_city"),
    )
    .orderBy("category")
)

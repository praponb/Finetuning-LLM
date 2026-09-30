from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Chiang Mai", "Bangkok", "London"]))
    .select(
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("s.city"),
    )
    .orderBy(F.col("cost").desc())
)

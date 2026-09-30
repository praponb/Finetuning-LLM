from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["TH", "US", "SG"]) & (F.col("p.cost") <= 450))
    .select(
        F.col("p.category"),
        F.col("s.supplier_name"),
        F.upper(F.col("s.country")).alias("country_upper"),
    )
    .orderBy(F.col("category").desc(), "supplier_name")
)

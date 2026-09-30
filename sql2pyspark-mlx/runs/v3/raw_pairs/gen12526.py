from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.upper(F.trim(F.col("s.supplier_name"))) == "EUROGOODS GMBH")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)

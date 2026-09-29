from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.brand").isNotNull() & F.col("p.launch_date").between("2023-06-01", "2025-08-01"))
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("s.country"),
        F.concat_ws("-", F.col("s.supplier_name"), F.col("s.country")).alias("supplier_name_country_key"),
    )
)

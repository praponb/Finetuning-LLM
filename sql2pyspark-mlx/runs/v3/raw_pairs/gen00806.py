from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.rating") <= 3.4)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("s.rating") <= 3.2) & (F.col("p.cost") >= 670))
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.rating"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("p.price") <= 1050)
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("s.rating"),
    )
    .orderBy("country")
)

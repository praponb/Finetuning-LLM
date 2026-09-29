from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.price").between(430, 1200) & (F.col("s.city") == "London"))
    .select(
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
    .orderBy(F.col("brand").desc())
)

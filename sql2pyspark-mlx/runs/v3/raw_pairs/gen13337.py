from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.price").between(800, 1150))
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)

from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.cost").between(530, 720) & (F.col("p.cost") <= 540))
    .select(
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)

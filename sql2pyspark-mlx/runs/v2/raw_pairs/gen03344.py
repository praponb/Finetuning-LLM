from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.supplier_name").like("Gl%"))
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy("id")
)

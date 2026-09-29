from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.supplier_name").like("%Sia%"))
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
    )
)

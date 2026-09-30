from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.price").between(360, 1040))
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
    .orderBy("city", "id")
    .limit(10)
)

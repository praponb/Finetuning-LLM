from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Tokyo", "Chiang Mai"]) & (F.col("p.cost") > 640))
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
    .orderBy("supplier_name", "id")
    .limit(10)
)

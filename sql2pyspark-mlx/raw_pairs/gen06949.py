from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("city") == "Singapore").alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.category").isin(["Books", "Electronics"]))
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.supplier_id"))
    .orderBy(F.col("cost").desc(), "id")
    .limit(10)
)

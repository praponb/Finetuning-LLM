from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("category") == "Books").alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.city") == "Singapore")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.rating"), F.col("s.city"))
    .orderBy(F.col("id").desc(), "country")
    .limit(5)
)

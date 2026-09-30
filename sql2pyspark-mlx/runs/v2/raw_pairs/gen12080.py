from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(~F.col("category").isin(["Electronics", "Books"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.city") != "London")
    .select(F.col("s.id"), F.col("s.country"))
    .orderBy(F.col("country").desc(), "id")
    .limit(1)
)

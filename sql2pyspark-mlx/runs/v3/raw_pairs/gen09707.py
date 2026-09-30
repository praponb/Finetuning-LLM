from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select("id", "tags", (F.col("cost") * F.col("price")).alias("margin"))
    .orderBy(F.col("margin").desc(), "id")
    .limit(5)
)

from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller") & (F.col("price") <= 790))
    .orderBy(F.col("id").desc())
    .limit(3)
)

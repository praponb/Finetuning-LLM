from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Books", "Electronics"]))
    .select("category", "brand")
    .distinct()
    .orderBy(F.col("category").desc(), "brand")
    .limit(50)
)

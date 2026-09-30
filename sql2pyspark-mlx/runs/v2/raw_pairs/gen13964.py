from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Fashion")
    .select("id", "brand")
    .orderBy(F.col("id").desc(), "brand")
    .limit(5)
)

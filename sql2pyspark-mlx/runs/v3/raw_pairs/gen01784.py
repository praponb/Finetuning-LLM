from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Fashion")
    .select("brand", "category")
    .distinct()
    .orderBy("category", "brand")
    .limit(5)
)

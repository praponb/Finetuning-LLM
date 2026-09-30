from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
    .filter(F.col("price_range") >= 79)
    .select("category", "brand", "price_range", "sports_count")
    .orderBy("brand")
)

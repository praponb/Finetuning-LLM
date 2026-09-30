from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Sports", F.col("price")).otherwise(0)).alias("sports_price"),
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
    )
    .filter(F.col("home_count") < 69)
    .orderBy(F.col("sports_price").desc())
)

from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 350)
    .groupBy("brand")
    .agg(F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"))
    .filter(F.col("home_count") >= 74)
    .select("brand", "home_count")
    .orderBy("brand")
)

from pyspark.sql import functions as F

src = df_products.filter((F.col("cost") >= 320) | (F.col("category") != "Grocery"))

products_stats = (
    src
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)

result = (
    products_stats
    .select("category", "initech_price", "umbrella_count")
    .orderBy(F.col("initech_price").desc(), "category")
    .limit(20)
)

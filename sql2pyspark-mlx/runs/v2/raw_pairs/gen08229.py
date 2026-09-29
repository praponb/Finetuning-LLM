from pyspark.sql import functions as F

recent_products = df_products.filter(F.array_contains("tags", "eco"))

agg = (
    recent_products
    .groupBy("category")
    .agg(
        F.max("cost").alias("max_cost"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
    )
)

result = (
    agg
    .filter(F.col("umbrella_count") > 3)
    .orderBy(F.col("max_cost").desc(), "category")
    .limit(5)
)

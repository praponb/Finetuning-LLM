from pyspark.sql import functions as F

src = df_products.filter((F.col("price") <= 670) & F.array_contains("tags", "sale"))

products_stats = (
    src
    .groupBy("category")
    .agg(F.count("price").alias("price_count"))
)

result = (
    products_stats
    .select("category", "price_count")
    .orderBy(F.col("price_count").desc())
)

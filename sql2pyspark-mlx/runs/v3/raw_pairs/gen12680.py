from pyspark.sql import functions as F

src = df_products.filter(F.col("brand") == "Acme")

products_stats = (
    src
    .groupBy("brand")
    .agg(F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"))
)

result = (
    products_stats
    .filter(F.col("fashion_count") > 5)
    .select("brand", "fashion_count")
)

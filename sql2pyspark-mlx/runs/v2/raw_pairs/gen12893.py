from pyspark.sql import functions as F

recent_products = df_products.filter(F.array_contains("tags", "clearance") | (F.col("price") >= 760))

summary = (
    recent_products
    .groupBy("brand")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("price").alias("price_count"),
    )
)

result = (
    summary
    .filter(F.col("avg_cost") >= 440)
    .select("brand", "avg_cost", "price_count")
)

from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("cost").between(370, 510) & (F.col("category") == "Electronics"))
)

products_stats = (
    base
    .groupBy(
        F.when(F.col("price") >= 1220, "high").when(F.col("price") >= 460, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.max("price").alias("max_price"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    products_stats
    .orderBy(F.col("cost_count").desc(), "price_bucket")
    .limit(10)
)

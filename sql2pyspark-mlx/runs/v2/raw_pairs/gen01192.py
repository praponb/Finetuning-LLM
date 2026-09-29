from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Sports") & F.col("price").between(360, 970) & F.col("brand").isNull())
    .groupBy("category")
    .agg(
        F.avg("price").alias("avg_price"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
    )
    .orderBy(F.col("avg_price").desc(), "category")
    .limit(50)
)

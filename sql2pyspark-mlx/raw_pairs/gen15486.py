from pyspark.sql import functions as F

base = df_products.filter(F.col("brand").isNotNull())

products_stats = (
    base
    .groupBy(
        F.when(F.col("price") >= 910, "high").when(F.col("price") >= 370, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = products_stats.select("price_bucket", "avg_cost")

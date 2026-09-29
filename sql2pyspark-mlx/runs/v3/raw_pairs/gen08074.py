from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1260, "high").when(F.col("price") >= 340, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.sum("cost").alias("total_cost"))
)

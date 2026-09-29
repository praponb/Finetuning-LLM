from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.when(F.col("cost") >= 550, "high").when(F.col("cost") >= 190, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") > 350)
)

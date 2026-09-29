from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 640, "high").when(F.col("cost") >= 270, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") >= 460)
    .orderBy("max_cost")
)

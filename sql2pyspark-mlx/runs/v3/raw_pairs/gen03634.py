from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 690, "high").when(F.col("cost") >= 290, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .filter(F.col("avg_price") >= 43)
    .orderBy("cost_bucket")
    .limit(20)
)

from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 740, "high").when(F.col("cost") >= 240, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("price").alias("min_price"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
    .filter(F.col("stddev_price") >= 67)
    .orderBy("min_price")
)

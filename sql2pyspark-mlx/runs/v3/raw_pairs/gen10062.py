from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") >= 510)
    .orderBy("category", "stddev_cost")
)

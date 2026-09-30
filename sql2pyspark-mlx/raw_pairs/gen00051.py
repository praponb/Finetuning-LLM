from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 640)
    .groupBy("brand")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count("*").alias("total_count"),
        F.min("cost").alias("min_cost"),
    )
    .orderBy(F.col("stddev_cost").desc(), "min_cost")
)

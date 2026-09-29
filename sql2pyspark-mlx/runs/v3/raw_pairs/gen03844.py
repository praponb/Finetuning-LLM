from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("brand", "category")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("cost").alias("max_cost"),
    )
    .orderBy(F.col("stddev_cost").desc(), "brand", "category")
    .limit(50)
)

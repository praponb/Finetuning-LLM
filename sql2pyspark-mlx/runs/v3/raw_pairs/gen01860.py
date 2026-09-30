from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(440, 580))
    .groupBy(
        "category",
        F.when(F.col("price") >= 1300, "high").when(F.col("price") >= 240, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("cost_range") >= 58)
)

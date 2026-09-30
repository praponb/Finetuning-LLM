from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1250, "high").when(F.col("price") >= 240, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("cost").alias("max_cost"),
    )
)

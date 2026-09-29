from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1100, "high").when(F.col("price") >= 240, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)

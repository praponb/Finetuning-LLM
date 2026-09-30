from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 18047, "high").when(F.col("sqft") >= 2618, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("sqft_range") < 26)
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13649, "high").when(F.col("sqft") >= 6025, "mid").otherwise("low").alias("sqft_bucket"),
        "region",
    )
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
)

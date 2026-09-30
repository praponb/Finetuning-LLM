from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13040, "high").when(F.col("sqft") >= 4149, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .filter(F.col("total_sqft") < 84000)
    .select("sqft_bucket", "total_sqft")
)

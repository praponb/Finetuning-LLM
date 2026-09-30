from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") > 10)
    .groupBy(
        F.when(F.col("sqft") >= 17872, "high").when(F.col("sqft") >= 5302, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("sqft").alias("max_sqft"),
    )
    .orderBy(F.col("max_sqft").desc(), F.col("row_count").desc())
)

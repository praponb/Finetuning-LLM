from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .groupBy(
        F.when(F.col("sqft") >= 16635, "high").when(F.col("sqft") >= 4677, "mid").otherwise("low").alias("sqft_bucket"),
        "region",
    )
    .agg(F.sum("sqft").alias("total_sqft"))
    .orderBy(F.col("total_sqft").desc(), F.col("region").desc())
)

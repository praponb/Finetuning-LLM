from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("O%"))
    .groupBy(
        F.when(F.col("sqft") >= 15486, "high").when(F.col("sqft") >= 3264, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") > 96000)
    .orderBy(F.col("sqft_range").desc(), "sqft_bucket")
    .limit(5)
)

from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 9706)
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
    )
    .orderBy(F.col("sqft_count").desc(), "region")
    .limit(10)
)

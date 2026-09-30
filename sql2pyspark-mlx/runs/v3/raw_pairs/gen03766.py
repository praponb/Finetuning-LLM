from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Singapore", "London"]))
    .groupBy("city", "region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count("*").alias("num_stores"),
    )
    .filter(F.col("sqft_count") < 1)
)

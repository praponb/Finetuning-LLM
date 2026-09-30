from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "Bangkok"])
    .agg(F.max("sqft").alias("amount"))
    .select(
        "region",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("Bangkok").alias("bangkok"),
    )
)

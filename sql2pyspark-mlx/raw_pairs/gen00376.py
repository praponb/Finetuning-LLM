from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .orderBy(F.col("unique_regions").desc(), "region")
)

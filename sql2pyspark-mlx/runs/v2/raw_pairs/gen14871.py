from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .filter(F.col("east_count") > 1)
    .orderBy(F.col("east_count").desc())
)

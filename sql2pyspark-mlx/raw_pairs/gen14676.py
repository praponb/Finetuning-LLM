from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
    )
    .orderBy("unique_regions")
)

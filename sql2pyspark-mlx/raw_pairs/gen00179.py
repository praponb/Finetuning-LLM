from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Singapore")
    .groupBy("region", "city")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.sum(F.when(F.col("city") == "London", F.col("sqft")).otherwise(0)).alias("london_sqft"),
    )
)

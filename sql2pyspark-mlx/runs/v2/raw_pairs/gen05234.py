from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.avg("sqft").alias("avg_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)

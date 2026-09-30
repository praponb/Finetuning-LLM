from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 4673) & (F.col("sqft") <= 14640))
    .groupBy("city", "region")
    .agg(F.countDistinct("sqft").alias("unique_regions"))
    .filter(F.col("unique_regions") > 4)
    .orderBy(F.col("region").desc())
)

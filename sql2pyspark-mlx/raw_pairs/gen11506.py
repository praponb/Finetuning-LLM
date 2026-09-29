from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.min("sqft").alias("min_sqft"))
    .filter(F.col("min_sqft") < 15300)
    .select("region", "min_sqft")
    .orderBy(F.col("min_sqft").desc())
)

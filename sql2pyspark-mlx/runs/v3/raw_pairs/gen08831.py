from pyspark.sql import functions as F

result = (
    df_stores
    .agg(F.countDistinct("sqft").alias("unique_regions"))
    .orderBy(F.col("unique_regions").desc())
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.countDistinct("sqft").alias("unique_regions"))
    .filter(F.col("unique_regions") < 9)
    .orderBy(F.col("opened_quarter").desc())
    .limit(1)
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("max_sqft") < 11700)
    .orderBy(F.col("region").desc())
)

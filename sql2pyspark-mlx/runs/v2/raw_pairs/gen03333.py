from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)

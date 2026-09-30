from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") > 5)
    .orderBy("opened_quarter")
    .limit(20)
)

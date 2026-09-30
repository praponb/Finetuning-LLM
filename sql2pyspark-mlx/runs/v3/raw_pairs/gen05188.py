from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") < 5)
    .orderBy("sqft_count")
)

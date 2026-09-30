from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Express%") & (F.length("store_name") >= 14))
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
)

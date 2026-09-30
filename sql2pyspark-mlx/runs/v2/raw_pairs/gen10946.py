from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "New York")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .filter(F.col("unique_regions") < 4)
)

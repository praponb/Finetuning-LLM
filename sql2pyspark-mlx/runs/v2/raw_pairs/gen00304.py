from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.month("opened_date").isin([7, 8, 12]))

stores_stats = (
    valid_stores
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
)

result = (
    stores_stats
    .select("region", "opened_quarter", "max_sqft", "unique_regions")
    .orderBy("region", "max_sqft")
)

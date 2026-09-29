from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") > 14200)
)

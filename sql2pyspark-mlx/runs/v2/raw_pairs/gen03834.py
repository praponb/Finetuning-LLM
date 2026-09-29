from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
    .filter(F.col("west_sqft") < 50)
    .select("opened_quarter", "city_list", "west_sqft")
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.count("sqft").alias("sqft_count"),
    )
)

from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "East") & (F.col("sqft") > 14550) & F.col("store_name").like("Air%"))
    .groupBy("city", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.countDistinct("sqft").alias("unique_citys"),
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
    )
    .filter(F.col("north_count") >= 48)
    .drop("north_count")
    .orderBy("unique_citys")
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("country") == "UK", F.col("rating"))).alias("uk_total_rating"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("uk_total_rating") >= 0)
    .orderBy("country")
)

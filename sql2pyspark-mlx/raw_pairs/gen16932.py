from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.max("opened_date").alias("latest_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .filter(F.col("new_york_count") >= 73)
    .drop("new_york_count")
    .orderBy("city_values", F.col("city").desc())
    .limit(10)
)

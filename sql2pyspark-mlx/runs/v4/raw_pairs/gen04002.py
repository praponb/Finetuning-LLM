from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("total_count").desc(), "city")
    .limit(3)
)

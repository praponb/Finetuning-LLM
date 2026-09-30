from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("tokyo_count").desc(), "city")
    .limit(50)
)

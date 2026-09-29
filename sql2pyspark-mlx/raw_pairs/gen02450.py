from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum("sqft").alias("total_sqft"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .orderBy("distinct_cities")
)

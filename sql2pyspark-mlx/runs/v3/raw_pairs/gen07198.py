from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.countDistinct("sqft").alias("unique_regions"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy("unique_regions", "city")
    .limit(1)
)

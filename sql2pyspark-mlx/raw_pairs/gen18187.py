from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .orderBy("city", F.col("stddev_sqft").desc())
    .limit(20)
)

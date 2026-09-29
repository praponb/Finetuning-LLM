from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 15191)
    .groupBy("region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy("region")
    .limit(1)
)

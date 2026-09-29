from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("city").desc())
    .limit(20)
)

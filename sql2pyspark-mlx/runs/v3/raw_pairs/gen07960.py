from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("store_name").isNotNull())

agg = (
    valid_stores
    .groupBy(
        F.when(F.col("city").isin(["London", "Berlin"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)

result = (
    agg
    .filter(F.col("stddev_sqft") >= 44)
    .orderBy(F.col("city_group").desc())
)

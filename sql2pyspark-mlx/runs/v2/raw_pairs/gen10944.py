from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2023-08-01")
    .groupBy("city", "region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)

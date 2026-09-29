from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull())
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"), "city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)

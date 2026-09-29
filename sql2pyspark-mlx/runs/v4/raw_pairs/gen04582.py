from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", "city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.max("sqft").alias("max_sqft"),
    )
)

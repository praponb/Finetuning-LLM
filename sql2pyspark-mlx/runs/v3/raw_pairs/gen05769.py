from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)

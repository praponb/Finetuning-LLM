from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") >= 1)
)

from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Singapore") & (F.col("city") == "Chiang Mai"))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.avg("sqft").alias("avg_sqft"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
)

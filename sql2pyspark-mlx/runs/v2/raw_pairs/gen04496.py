from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") < 58)
    .select("city", "region_list", "avg_sqft")
)

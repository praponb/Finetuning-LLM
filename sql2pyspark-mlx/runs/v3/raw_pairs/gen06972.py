from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 5100)
    .select("region", "city_list", "avg_sqft")
    .orderBy(F.col("avg_sqft").desc())
)

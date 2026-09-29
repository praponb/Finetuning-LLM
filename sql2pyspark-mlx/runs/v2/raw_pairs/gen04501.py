from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count("*").alias("num_stores"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
    .orderBy(F.col("city").desc())
    .limit(5)
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 14266, "high").when(F.col("sqft") >= 5859, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("avg_sqft") < 14500)
    .orderBy(F.col("avg_sqft").desc())
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16574, "high").when(F.col("sqft") >= 4352, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)

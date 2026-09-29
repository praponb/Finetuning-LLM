from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 14568, "high").when(F.col("sqft") >= 3166, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum("sqft").alias("total_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") >= 9600)
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 18041, "high").when(F.col("sqft") >= 4144, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .filter(F.col("chiang_mai_count") < 5)
)

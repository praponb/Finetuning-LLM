from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 17543, "high").when(F.col("sqft") >= 6440, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("sqft_bucket").desc())
)

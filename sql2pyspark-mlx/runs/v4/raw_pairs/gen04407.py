from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.col("sqft") <= 6832) & F.col("region").isin(["South", "West", "East"]))
)

stores_stats = (
    recent_stores
    .groupBy("region")
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)

result = (
    stores_stats
    .filter(F.col("unique_citys") < 6)
    .select("region", "unique_citys", "region_list")
    .orderBy(F.col("unique_citys").desc())
)

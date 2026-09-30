from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
    )
    .orderBy("region")
)

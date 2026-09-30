from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)

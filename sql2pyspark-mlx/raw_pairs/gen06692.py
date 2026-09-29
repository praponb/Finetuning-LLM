from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
    .orderBy("pct_east")
)

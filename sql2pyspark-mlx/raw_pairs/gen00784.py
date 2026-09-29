from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
    .filter(F.col("singapore_count") >= 58)
    .orderBy(F.col("singapore_count").desc(), "opened_quarter")
    .limit(20)
)

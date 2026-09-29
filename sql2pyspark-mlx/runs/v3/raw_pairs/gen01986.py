from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("store_name").like("%Downtown%") | (F.col("region") == "East"))
)

summary = (
    recent_stores
    .groupBy("region")
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
)

result = (
    summary
    .select("region", "region_list")
    .orderBy("region")
)

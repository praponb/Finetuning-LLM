from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
    .orderBy("city")
)

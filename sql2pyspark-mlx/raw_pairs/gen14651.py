from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
)

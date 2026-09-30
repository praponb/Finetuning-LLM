from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "Central", "South"]))
    .groupBy("city")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
    .orderBy("city")
)

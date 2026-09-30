from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "host", "level", F.explode("context").alias("k", "value"))
)

result = (
    flattened
    .groupBy("host", "k")
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .filter(F.col("num_levels") > 1)
)

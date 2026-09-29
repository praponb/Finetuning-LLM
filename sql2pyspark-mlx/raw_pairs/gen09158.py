from pyspark.sql import functions as F

result = (
    df_sessions
    .cube("browser")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.grouping("browser").alias("grouping_id"),
    )
)

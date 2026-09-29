from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "host", F.explode_outer("context").alias("attr_key", "v"))
)

result = (
    flattened
    .groupBy("attr_key")
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.countDistinct("service").alias("distinct_services"),
    )
    .orderBy(F.col("num_services").desc_nulls_last())
)

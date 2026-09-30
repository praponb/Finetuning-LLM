from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "host", F.explode("context").alias("attr_key", "v"))
)

result = (
    flattened
    .cube("service")
    .agg(
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
        F.count_if(F.col("attr_key") == "trace").alias("trace_events"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("num_hosts") > 1)
)

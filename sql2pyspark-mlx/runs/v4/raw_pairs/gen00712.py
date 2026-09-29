from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .groupBy("host")
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
    )
    .filter(F.col("region_rows") >= 1)
    .orderBy(F.col("distinct_hosts").desc())
)

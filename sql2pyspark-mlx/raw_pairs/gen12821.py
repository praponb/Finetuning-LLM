from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "log_ts", "level", F.explode("context").alias("key", "v"))
)

result = (
    flattened
    .groupBy("service")
    .agg(
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
        F.size(F.collect_set("service")).alias("num_services"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min_2"),
    )
)

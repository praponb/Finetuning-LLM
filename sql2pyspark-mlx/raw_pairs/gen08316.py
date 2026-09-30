from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.max("log_ts").alias("ended_at"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_rows"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.min("log_ts").alias("started_at"),
        F.size(F.collect_set("host")).alias("num_hosts_2"),
        F.size(F.collect_set("host")).alias("num_hosts_3"),
    )
)

from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .groupBy("level", F.date_trunc("DAY", "log_ts").alias("activity_day"), "k")
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count_if(F.col("k") == "version").alias("version_events"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("activity_day")
)

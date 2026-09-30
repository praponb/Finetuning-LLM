from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 429, "LARGE").otherwise("SMALL"))
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .rollup("status_code_tier", "hour_window")
    .agg(
        F.min("log_ts").alias("started_at"),
        F.sum(F.when(F.col("attr_key") == "version", F.col("status_code")).otherwise(0)).alias("version_status_code"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.grouping_id().alias("gid"),
    )
)

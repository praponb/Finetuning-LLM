from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("k") == "version", F.col("status_code")).otherwise(0)).alias("version_status_code"),
    )
    .orderBy(F.col("num_levels_2").desc())
)

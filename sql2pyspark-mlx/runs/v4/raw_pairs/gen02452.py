from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.max("log_ts").alias("ended_at"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.max("log_ts").alias("last_seen"),
    )
    .orderBy(F.col("span_minutes").desc())
)

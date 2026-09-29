from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "payload_json",
        "context",
        F.coalesce(F.col("context").getItem("region"), F.lit("unknown")).alias("region_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
    )
)

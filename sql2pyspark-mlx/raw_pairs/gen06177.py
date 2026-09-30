from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    flattened_attrs
    .cube("host")
    .agg(
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_count"),
        F.sum("flag_bit1").alias("total_flag_bit1"),
        F.max("log_ts").alias("last_log_ts"),
        F.percentile_approx("flag_bit1", 0.95).alias("flag_bit1_p95"),
        F.count_if(F.col("host") == "h3").alias("h3_events"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("h3_events")
)

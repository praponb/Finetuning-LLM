from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .cube("hour_window")
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.sum(F.when(F.col("attr_key") == "version", F.col("latency_ms")).otherwise(0)).alias("version_latency_ms"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.corr("latency_ms", "status_code").alias("corr_latency_ms_status_code"),
        F.min("log_ts").alias("first_log_ts_2"),
        F.max(F.when(F.col("attr_key") == "version", F.col("status_code"))).alias("version_max_status_code"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("status_code_bit_and").desc_nulls_first())
)

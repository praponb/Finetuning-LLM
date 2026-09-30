from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_rows"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
    )
    .orderBy(F.col("activity_day").asc_nulls_first())
)

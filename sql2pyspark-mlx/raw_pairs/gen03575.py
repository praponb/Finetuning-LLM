from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .groupBy("service")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
    )
    .orderBy(F.col("search_count").desc_nulls_last())
)

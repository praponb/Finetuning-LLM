from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .groupBy("attr_key", "service")
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.sum(F.when(F.col("attr_key") == "version", F.col("status_code")).otherwise(0)).alias("version_status_code"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows_2"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "auth", 1)), F.lit(0)), 2).alias("status_code_per_auth"),
    )
    .orderBy(F.col("status_code_per_auth").asc_nulls_first())
)

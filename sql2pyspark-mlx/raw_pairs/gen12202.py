from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.col("context")["region"].alias("region"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("status_code") >= 407, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.max("log_ts").alias("ended_at"),
    )
    .filter(F.col("auth_rows") < 1000)
    .orderBy(F.col("auth_rows").desc_nulls_last())
)

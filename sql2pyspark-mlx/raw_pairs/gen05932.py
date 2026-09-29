from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(-3, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .filter(F.col("service").isin(["billing", "auth", "checkout"]))
    .select(
        "id",
        "service",
        "log_ts",
        "version_or_default",
        "has_version",
        F.sum("status_code").over(w_recent).alias("last4_sum_status_code"),
        F.min("status_code").over(w_recent).alias("last4_min_status_code"),
        F.max("status_code").over(w_recent).alias("last4_max_status_code"),
    )
)

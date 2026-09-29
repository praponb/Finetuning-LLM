from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w).alias("seq_no"),
    )
)

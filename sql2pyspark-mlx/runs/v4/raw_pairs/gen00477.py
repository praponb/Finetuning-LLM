from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level")
w_grp = Window.partitionBy("level").orderBy("latency_ms", "id")

parsed = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-06-23")
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w_running).alias("corr_status_code_latency_ms"),
        F.lead("status_code", 1).over(w_grp).alias("next_status_code"),
        F.lead("latency_ms", 1).over(w_grp).alias("next_latency_ms"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "host",
        "status_code",
        "version",
        "user_id",
        F.lead("latency_ms").over(w).alias("next_latency_ms"),
        F.lag("latency_ms").over(w).alias("prev_latency_ms"),
        F.lead("status_code").over(w).alias("next_status_code"),
        F.row_number().over(w).alias("row_seq"),
    )
)

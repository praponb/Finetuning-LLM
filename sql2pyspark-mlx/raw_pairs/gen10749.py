from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.year("log_ts"))
w2 = Window.partitionBy("service")

unpacked = (
    df_app_logs
    .filter(F.col("service") != "search")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.sum("latency_ms").over(w_running).alias("log_ts_year_sum_latency_ms"),
        F.max("status_code").over(w2).alias("service_max_status_code"),
    )
)

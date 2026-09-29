from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context")["version"].alias("version"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.col("context").getItem("region").alias("region"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service").orderBy(F.col("num_context").desc())
w_grp = Window.partitionBy(F.year("log_ts"), "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "payload_json",
        F.col("context")["trace"].alias("trace"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
    )
)

result = (
    parsed
    .filter(F.col("log_ts") < "2025-02-04")
    .select(
        "id",
        "has_version",
        F.percent_rank().over(w).alias("num_context_percent_rank"),
        F.avg("num_context").over(w_grp).alias("num_context_avg_30m"),
    )
)

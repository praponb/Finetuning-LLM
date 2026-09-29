from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("session_id", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

flagged = (
    exploded
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

valid = (
    sessions_app_logs
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 14:00:00"))) & (~F.col("attr_key").eqNullSafe(F.lit("region"))))
    .select(
        "*",
        F.date_trunc("week", "log_ts").alias("log_ts_period"),
        F.col("session_start_flag").cast("string").alias("session_start_flag_num"),
    )
)

base = (
    valid
    .filter((F.col("log_ts") >= F.lit("2025-06-01 11:00:00").cast("timestamp")) & (F.col("attr_key").eqNullSafe(F.lit("version"))))
    .select(
        "*",
        F.least("session_id", "status_code").alias("least_session_id_status_code"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "attr_value",
        "session_id",
        "log_ts_period",
        "least_session_id_status_code",
    )
)

result = (
    deduped
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "session_id",
        "log_ts_period",
        "least_session_id_status_code",
    )
)

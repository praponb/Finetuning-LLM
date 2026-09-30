from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("log_ts"), "attr_key").orderBy(F.col("status_code_high").desc_nulls_first())
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.orderBy(F.col("status_code_high_dense_rank").desc())
w4 = Window.orderBy(F.col("status_code_high_dense_rank_rank").desc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .select(
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
    )
)

scored = (
    parsed
    .filter(F.col("latency_ms").between(1068, 1920))
    .select(
        "latency_ms",
        "attr_value",
        "status_code_high",
        F.dense_rank().over(w).alias("status_code_high_dense_rank"),
    )
)

scored_app_logs = (
    scored
    .select(
        "latency_ms",
        "attr_value",
        "status_code_high",
        F.rank().over(w2).alias("latency_ms_rank"),
        F.rank().over(w3).alias("status_code_high_dense_rank_rank"),
    )
)

scored_app_logs_2 = (
    scored_app_logs
    .select(
        "*",
        F.dense_rank().over(w4).alias("status_code_high_dense_rank_rank_dense_rank"),
    )
)

result = (
    scored_app_logs_2
    .select(
        "latency_ms",
        "attr_value",
        "status_code_high_dense_rank_rank",
        "status_code_high_dense_rank_rank_dense_rank",
    )
)

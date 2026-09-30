from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy("status_code_high")
w3 = Window.orderBy(F.col("has_fault_plus_status_code_high").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    lines
    .filter(F.col("status_code") > 380)
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

filtered = (
    parsed
    .filter(F.col("log_ts") < "2023-07-01")
    .select(
        "*",
        (F.col("has_fault") + F.col("status_code_high")).alias("has_fault_plus_status_code_high"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.sum("status_code").over(w).alias("status_code_sum_1h"),
        F.percent_rank().over(w2).alias("status_code_high_percent_rank"),
        F.percent_rank().over(w3).alias("has_fault_plus_status_code_high_percent_rank"),
    )
)

filtered_app_logs = (
    with_windows
    .filter(F.col("status_code_sum_1h") < 1701)
    .select(
        "id",
        "attr_value",
        "has_fault",
        "status_code_sum_1h",
        "status_code_high_percent_rank",
        "has_fault_plus_status_code_high_percent_rank",
        (F.col("has_fault_plus_status_code_high_percent_rank") + F.col("status_code_sum_1h")).alias("has_fault_plus_status_code_high_percent_rank_plus_status_code_sum_1h"),
    )
)

result = (
    filtered_app_logs
    .select(
        "id",
        "status_code_high_percent_rank",
        "has_fault_plus_status_code_high_percent_rank",
        "has_fault_plus_status_code_high_percent_rank_plus_status_code_sum_1h",
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc())
w2 = Window.orderBy("status_code")
w3 = Window.orderBy(F.col("status_code_high").asc_nulls_first())
w4 = Window.orderBy(F.col("status_code_high").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

unpacked = (
    lines
    .filter(F.col("status_code") >= 363)
    .select(
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("has_fault"),
    )
)

scored = (
    unpacked
    .select(
        "*",
        F.cume_dist().over(w).alias("status_code_high_cume_dist"),
        F.rank().over(w2).alias("status_code_rank"),
        F.percent_rank().over(w3).alias("status_code_high_percent_rank"),
        F.rank().over(w4).alias("status_code_high_rank"),
        F.dense_rank().over(w4).alias("status_code_high_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("status_code_high_percent_rank") > 0.22)
    .select(
        "status_code",
        "status_code_high",
        "has_fault",
        "status_code_high_cume_dist",
        "status_code_rank",
        "status_code_high_dense_rank",
        F.coalesce("status_code_high_dense_rank", F.lit(0)).alias("status_code_high_dense_rank_filled"),
        (F.col("status_code") * F.col("status_code_high_percent_rank")).alias("status_code_times_status_code_high_percent_rank"),
    )
)

filtered = (
    valid
    .filter(F.col("status_code_times_status_code_high_percent_rank") <= 75)
    .withColumn("status_code_times_status_code_high_percent_rank_whole", F.floor("status_code_times_status_code_high_percent_rank"))
)

result = (
    filtered
    .select(
        "status_code",
        "status_code_high",
        "has_fault",
        "status_code_high_cume_dist",
        "status_code_rank",
        "status_code_high_dense_rank",
        "status_code_high_dense_rank_filled",
        "status_code_times_status_code_high_percent_rank_whole",
    )
)

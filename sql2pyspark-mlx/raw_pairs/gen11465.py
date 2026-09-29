from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_hist = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy(F.col("status_code_dense_rank").asc_nulls_last())
w6 = Window.orderBy(F.col("status_code_dense_rank_cume_dist").asc_nulls_last())
w7 = Window.orderBy(F.col("status_code_high_max_2h_per_status_code_high_max_2h_max_15m").asc_nulls_first())
w8 = Window.orderBy(F.col("status_code_dense_rank_cume_dist").desc_nulls_first())
w9 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") != 280)
    .select(
        "log_ts",
        "status_code",
        "value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

enriched = (
    parsed
    .select(
        "log_ts",
        "value",
        F.dense_rank().over(w).alias("status_code_dense_rank"),
        F.max("status_code_high").over(w2).alias("status_code_high_max_2h"),
        F.count("flag_bit4").over(w3).alias("flag_bit4_count_30m"),
    )
)

with_windows = (
    enriched
    .select(
        "log_ts",
        "value",
        "status_code_high_max_2h",
        F.max("status_code_high_max_2h").over(w_hist).alias("status_code_high_max_2h_max_15m"),
        F.cume_dist().over(w5).alias("status_code_dense_rank_cume_dist"),
        F.max("status_code_dense_rank").over(w3).alias("status_code_dense_rank_max_30m"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "*",
        (F.col("status_code_high_max_2h") / F.nullif("status_code_high_max_2h_max_15m", F.lit(0))).alias("status_code_high_max_2h_per_status_code_high_max_2h_max_15m"),
        F.weekofyear("log_ts").alias("log_ts_weekofyear"),
    )
)

scored = (
    cleaned
    .select(
        "log_ts",
        "status_code_high_max_2h",
        "status_code_high_max_2h_max_15m",
        "status_code_dense_rank_cume_dist",
        "log_ts_weekofyear",
        F.dense_rank().over(w6).alias("status_code_dense_rank_cume_dist_dense_rank"),
        F.rank().over(w7).alias("status_code_high_max_2h_per_status_code_high_max_2h_max_15m_rank"),
        F.cume_dist().over(w8).alias("status_code_dense_rank_cume_dist_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 07:00:00")))
    .withColumn("status_code_high_max_2h_per_status_code_high_max_2h_max_15m_rank_per_status_code_high_max_2h_max_15m", (F.col("status_code_high_max_2h_per_status_code_high_max_2h_max_15m_rank") / F.nullif("status_code_high_max_2h_max_15m", F.lit(0))))
)

result = (
    filtered
    .select(
        "*",
        F.max("status_code_high_max_2h_max_15m").over(w9).alias("status_code_high_max_2h_max_15m_max_1h"),
        F.sum("status_code_dense_rank_cume_dist_dense_rank").over(w3).alias("status_code_dense_rank_cume_dist_dense_rank_sum_30m"),
    )
)

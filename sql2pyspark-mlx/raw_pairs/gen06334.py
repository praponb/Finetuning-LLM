from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc())
w2 = Window.partitionBy("service", "k").orderBy(F.col("status_code_high").asc_nulls_last())
w3 = Window.partitionBy("k").orderBy(F.col("is_flagged").desc())
w4 = Window.partitionBy("k", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

unpacked = (
    exploded
    .select(
        "service",
        "log_ts",
        "k",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

scored = (
    unpacked
    .filter(F.col("status_code_high") != 2)
    .select(
        "service",
        "log_ts",
        "k",
        F.dense_rank().over(w).alias("status_code_high_dense_rank"),
        F.dense_rank().over(w2).alias("status_code_high_dense_rank_2"),
        F.cume_dist().over(w3).alias("is_flagged_cume_dist"),
        F.max("is_flagged").over(w4).alias("is_flagged_max_1h"),
    )
)

filtered = (
    scored
    .filter((F.col("log_ts") > F.to_date(F.lit("2025-06-01"))) & (~F.col("k").eqNullSafe(F.lit("version"))))
    .select(
        "service",
        "log_ts",
        "status_code_high_dense_rank_2",
        "is_flagged_max_1h",
        F.greatest("status_code_high_dense_rank_2", "is_flagged_max_1h").alias("greatest_status_code_high_dense_rank_2_is_flagged_max_1h"),
        F.round((F.col("is_flagged_max_1h") / F.col("status_code_high_dense_rank_2")), 2).alias("is_flagged_max_1h_per_status_code_high_dense_rank_2"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

matched_keys = (
    filtered
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.to_date("log_ts") >= "2024-11-01")
        .select("service")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "service",
        "log_ts",
        "status_code_high_dense_rank_2",
        "greatest_status_code_high_dense_rank_2_is_flagged_max_1h",
        "is_flagged_max_1h_per_status_code_high_dense_rank_2",
        "k_clean",
    )
    .orderBy(
        F.col("status_code_high_dense_rank_2").desc_nulls_last(),
        F.col("greatest_status_code_high_dense_rank_2_is_flagged_max_1h").desc(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("adjusted_status_code_high_rounded").desc_nulls_first())
w_recent = Window.orderBy(F.col("adjusted_status_code_high").asc_nulls_last())
w_hist = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w_ordered = Window.orderBy(F.col("adjusted_status_code_high").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

unpacked = (
    flattened
    .filter(F.col("latency_ms") == 652)
    .select(
        "id",
        "latency_ms",
        "v",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    unpacked
    .filter(F.col("status_code_high") > 2)
    .select(
        "id",
        "latency_ms",
        "v",
        "status_code_high",
        F.round((F.col("status_code_high") * 0.85), 2).alias("adjusted_status_code_high"),
        F.when(F.col("latency_ms") >= 2067, "High").when(F.col("latency_ms") >= 697, "Medium").otherwise("Low").alias("latency_ms_level"),
    )
)

cleaned = (
    valid
    .filter(F.col("latency_ms") >= 1333)
    .select(
        "id",
        "latency_ms",
        "v",
        "status_code_high",
        "adjusted_status_code_high",
        "latency_ms_level",
        F.round("adjusted_status_code_high", 2).alias("adjusted_status_code_high_rounded"),
    )
)

metrics = (
    cleaned
    .filter(F.col("adjusted_status_code_high").between(62, 68))
    .select(
        "*",
        F.rank().over(w).alias("adjusted_status_code_high_rounded_rank"),
        F.cume_dist().over(w_recent).alias("adjusted_status_code_high_cume_dist"),
        F.percent_rank().over(w_hist).alias("status_code_high_percent_rank"),
        F.percent_rank().over(w_hist).alias("status_code_high_percent_rank_2"),
        F.rank().over(w_ordered).alias("adjusted_status_code_high_rank"),
    )
)

cleaned_app_logs = (
    metrics
    .filter(F.col("adjusted_status_code_high_rank").between(4, 5))
    .select(
        "*",
        F.when(F.col("status_code_high") >= 2, "High").when(F.col("status_code_high") >= 1, "Medium").otherwise("Low").alias("status_code_high_band"),
    )
)

result = (
    cleaned_app_logs
    .groupBy(
        F.when(F.col("adjusted_status_code_high_rounded_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("adjusted_status_code_high_rounded_rank_tier"),
        F.when(F.col("status_code_high_percent_rank") >= 0.75, "PREMIUM").otherwise("STANDARD").alias("status_code_high_percent_rank_tier"),
        F.when(F.col("status_code_high") >= 2, "HIGH").otherwise("LOW").alias("status_code_high_tier"),
    )
    .agg(
        F.round(F.avg("status_code_high_percent_rank"), 2).alias("avg_status_code_high_percent_rank"),
        F.percentile_approx("adjusted_status_code_high_rank", [0.25, 0.5, 0.75]).alias("adjusted_status_code_high_rank_quartiles"),
    )
)

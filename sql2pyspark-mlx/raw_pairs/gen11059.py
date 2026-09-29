from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "key").orderBy("status_code")
w2 = Window.partitionBy("level", "key")
w3 = Window.partitionBy("level")
w_hist = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

with_windows = (
    exploded
    .withColumn("status_code_rank", F.rank().over(w))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w2))
)

enriched = (
    with_windows
    .filter(F.col("corr_latency_ms_status_code").between(-0.6, -0.5))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code_rank",
        F.count("status_code_rank").over(w3).alias("level_count_status_code_rank"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "log_ts",
        "level",
        "level_count_status_code_rank",
        F.percent_rank().over(w_hist).alias("latency_ms_percent_rank"),
        F.count("latency_ms").over(w3).alias("level_count_latency_ms"),
    )
)

filtered = (
    scored
    .filter(F.col("level") == "ERROR")
    .select(
        "*",
        F.round((F.col("level_count_status_code_rank") * 0.9), 2).alias("level_count_status_code_rank_scaled"),
    )
)

valid = (
    filtered
    .filter(F.col("level_count_latency_ms") > 13)
    .select(
        "level",
        "level_count_status_code_rank",
        "level_count_latency_ms",
        "level_count_status_code_rank_scaled",
        F.least("level_count_status_code_rank_scaled", "level_count_latency_ms").alias("least_level_count_status_code_rank_scaled_level_count_latency_ms"),
    )
)

scored_app_logs = (
    valid
    .select(
        "level",
        "level_count_status_code_rank_scaled",
        F.avg("least_level_count_status_code_rank_scaled_level_count_latency_ms").over(w3).alias("level_avg_least_level_count_status_code_rank_scaled_level_count_latency_ms"),
    )
)

result = (
    scored_app_logs
    .select("level_count_status_code_rank_scaled")
    .orderBy(F.col("level_count_status_code_rank_scaled").desc())
)

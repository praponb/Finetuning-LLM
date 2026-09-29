from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.orderBy(F.col("days_since_log_ts").asc_nulls_last())

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

cleaned = (
    flattened
    .filter(F.col("key").isNotNull() & (F.col("latency_ms") < 679))
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "key",
        "v",
        F.datediff(F.current_date(), "log_ts").alias("days_since_log_ts"),
        F.when(F.col("host").isin(["h4", "h1"]), "Tier 1").otherwise("Other").alias("host_group"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.rank().over(w).alias("status_code_rank"),
        F.percent_rank().over(w2).alias("days_since_log_ts_percent_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("key").eqNullSafe(F.lit("version")))
    .select(
        "id",
        "service",
        "host",
        "key",
        "v",
        "days_since_log_ts",
        "host_group",
        F.floor("status_code").alias("status_code_whole"),
        (F.col("status_code_rank") + F.col("days_since_log_ts_percent_rank")).alias("status_code_rank_plus_days_since_log_ts_percent_rank"),
    )
)

result = (
    filtered
    .select("service", "key", "v", "host_group", "status_code_whole")
    .orderBy("service")
)

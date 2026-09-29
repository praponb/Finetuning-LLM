from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level", "host").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code_plus_status_code_whole").asc_nulls_first())
w3 = Window.partitionBy("service", "host")
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.orderBy(F.col("status_code_whole").desc())
w_running = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w7 = Window.partitionBy(F.month("log_ts"), "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    exploded
    .filter(F.col("status_code") <= 434)
    .select("*", F.rank().over(w_recent).alias("latency_ms_rank"))
)

valid = (
    with_windows
    .filter(F.col("attr_key").isNotNull())
    .withColumn("host_level_key", F.concat_ws("-", "host", "level"))
    .withColumn("status_code_whole", F.floor("status_code"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("NONE")))
)

valid_app_logs = (
    valid
    .filter((~F.col("attr_key").eqNullSafe(F.lit("version"))) & F.col("attr_key").isNotNull())
    .withColumn("status_code_plus_status_code_whole", (F.col("status_code") + F.col("status_code_whole")))
    .withColumn("latency_ms_whole", F.floor("latency_ms"))
)

scored = (
    valid_app_logs
    .withColumn("status_code_plus_status_code_whole_cume_dist", F.cume_dist().over(w2))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w3))
    .withColumn("status_code_whole_max_30m", F.max("status_code_whole").over(w4))
    .withColumn("status_code_whole_cume_dist", F.cume_dist().over(w5))
)

result = (
    scored
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        "attr_value",
        "latency_ms_rank",
        "host_level_key",
        "status_code_plus_status_code_whole",
        "latency_ms_whole",
        "status_code_plus_status_code_whole_cume_dist",
        "status_code_whole_max_30m",
        "status_code_whole_cume_dist",
        F.avg("status_code_whole_max_30m").over(w_running).alias("status_code_whole_max_30m_avg_15m"),
        F.max("latency_ms_whole").over(w7).alias("latency_ms_whole_max_2h"),
    )
)

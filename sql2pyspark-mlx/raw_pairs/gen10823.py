from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code_high").desc_nulls_first())
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_first())
w_running = Window.partitionBy("host", "key").orderBy(F.col("status_code_high").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("latency_ms").between(1177, 1636))
    .select(
        "id",
        "host",
        "latency_ms",
        "key",
        "value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    flattened_attrs
    .filter(F.col("has_fault") <= 0)
    .withColumn("status_code_high_percent_rank", F.percent_rank().over(w))
    .withColumn("latency_ms_rank", F.rank().over(w2))
)

with_windows = (
    metrics
    .select(
        "host",
        "key",
        "value",
        "has_fault",
        F.cume_dist().over(w_running).alias("status_code_high_cume_dist"),
    )
)

result = (
    with_windows
    .select("host", "key", "has_fault", "status_code_high_cume_dist")
    .orderBy(F.col("key").desc())
)

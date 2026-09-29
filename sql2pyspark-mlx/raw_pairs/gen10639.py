from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("retry_count", "id")
w3 = Window.partitionBy("host").orderBy("latency_ms", "id")
w_grp = Window.partitionBy("host").orderBy(F.col("latency_ms").desc())
w5 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

unpacked = (
    df_app_logs
    .filter(F.col("host").isin(["h4", "h1"]))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "retry_count",
        "version",
        F.max("latency_ms").over(w).alias("latency_ms_max_30m"),
        F.lag("retry_count", 1).over(w2).alias("prev_retry_count"),
        F.lag("retry_count", 1, 0).over(w3).alias("prev_retry_count_2"),
        F.cume_dist().over(w_grp).alias("latency_ms_cume_dist"),
        F.sum("latency_ms").over(w5).alias("latency_ms_sum_2h"),
    )
)

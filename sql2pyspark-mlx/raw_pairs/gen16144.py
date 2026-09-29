from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("log_ts"), "host").orderBy(F.col("latency_ms").asc_nulls_last())
w2 = Window.partitionBy("host").orderBy("latency_ms", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("log_ts", "id")
w4 = Window.partitionBy(F.month("log_ts"), "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.percent_rank().over(w).alias("latency_ms_percent_rank"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_2"),
        F.lag("duration_sec", 1).over(w3).alias("prev_duration_sec"),
        F.sum("latency_ms").over(w4).alias("latency_ms_sum_30m"),
    )
)

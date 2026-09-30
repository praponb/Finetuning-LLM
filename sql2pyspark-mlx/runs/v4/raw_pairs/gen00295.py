from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("level").orderBy(F.col("retry_count").desc(), "id")
w3 = Window.partitionBy("level", "service").orderBy("latency_ms", "id")
w4 = Window.orderBy("retry_count", "id").rowsBetween(-2, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.col("host") == "h2")
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("latency_ms").over(w).alias("host_sum_latency_ms"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(5).over(w3).alias("latency_ms_bucket"),
        F.avg("retry_count").over(w4).alias("retry_count_avg_2"),
    )
)

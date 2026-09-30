from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("latency_ms", "id")
w3 = Window.orderBy("latency_ms", "id").rowsBetween(-3, -1)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .withColumn("cumulative_retry_count", F.sum("retry_count").over(w))
    .withColumn("latency_ms_bucket", F.ntile(3).over(w2))
    .withColumn("latency_ms_avg_3_prior", F.avg("latency_ms").over(w3))
)

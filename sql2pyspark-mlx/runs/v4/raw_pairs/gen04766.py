from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("tier")
w2 = Window.partitionBy("tier").orderBy("latency_ms", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("tier", "host").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2024-10-17")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w_part).alias("corr_status_code_latency_ms"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_3"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "host")
w2 = Window.partitionBy("service").orderBy(F.col("num_context").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context").getItem("region").alias("region"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "trace_or_default",
        "num_context",
        F.min("num_context").over(w).alias("service_min_num_context"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

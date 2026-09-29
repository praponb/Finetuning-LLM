from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id")
w3 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc(), "id")

flattened_attrs = (
    df_app_logs
    .filter(F.col("log_ts") < "2023-05-01")
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.col("context").getItem("trace").alias("trace"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("service").isin(["search", "billing"]))
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "has_fault",
        F.first("latency_ms", ignorenulls=True).over(w).alias("first_known_latency_ms"),
        F.ntile(3).over(w2).alias("status_code_bucket"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

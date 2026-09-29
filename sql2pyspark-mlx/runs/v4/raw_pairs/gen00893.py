from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code").desc(), "id")
w2 = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host", "service")

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms") <= 1193)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.coalesce(F.col("context").getItem("region"), F.lit("unknown")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .withColumn("status_code_bucket", F.ntile(5).over(w))
    .withColumn("last_known_status_code_high", F.last("status_code_high", ignorenulls=True).over(w2))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w3))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w4))
)

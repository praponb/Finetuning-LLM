from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("host").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms").between(652, 1938))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.col("context").getItem("region").alias("region"),
        F.size("context").alias("num_context"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "region",
        "num_context",
        "region_or_default",
        "duration_text",
        F.min("status_code_high").over(w_grp).alias("running_min_status_code_high"),
        F.avg("status_code_high").over(w_grp).alias("running_avg_status_code_high"),
        F.count("status_code_high").over(w_grp).alias("running_count_status_code_high"),
        F.sum("status_code_high").over(w_grp).alias("running_sum_status_code_high"),
    )
)

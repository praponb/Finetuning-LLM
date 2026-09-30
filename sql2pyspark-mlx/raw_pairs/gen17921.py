from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    unpacked
    .filter(F.col("service").isin(["search", "auth"]))
    .withColumn("running_sum_status_code", F.sum("status_code").over(w_part))
    .withColumn("running_count_status_code", F.count("status_code").over(w_part))
    .withColumn("running_avg_status_code", F.avg("status_code").over(w_part))
)

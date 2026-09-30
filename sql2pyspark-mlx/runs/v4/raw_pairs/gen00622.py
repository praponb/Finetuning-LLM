from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("service")
w2 = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("host")
w4 = Window.orderBy("retry_count", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("host").orderBy("retry_count")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["region"].alias("region"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.sum("status_code").over(w_hist).alias("service_sum_status_code"),
        F.percent_rank().over(w2).alias("status_code_percent_rank"),
        F.stddev_samp("has_fault").over(w3).alias("host_stddev_samp_has_fault"),
        F.avg("has_fault").over(w4).alias("has_fault_avg_3"),
        F.percent_rank().over(w5).alias("retry_count_percent_rank"),
    )
)

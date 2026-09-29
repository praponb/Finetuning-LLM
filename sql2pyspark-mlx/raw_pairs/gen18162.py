from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("log_ts")).orderBy(F.col("num_context").desc(), "id")
w2 = Window.partitionBy("service").orderBy("status_code", "id")
w3 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("service").orderBy("log_ts", "id")
w5 = Window.partitionBy(F.year("log_ts"), "service").orderBy("num_context", "id")

parsed = (
    df_app_logs
    .filter(F.to_date("log_ts") < "2025-01-01")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.size("context").alias("num_context"),
    )
)

result = (
    parsed
    .filter(F.col("status_code") >= 323)
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "has_region",
        "num_context",
        F.ntile(5).over(w).alias("num_context_bucket"),
        F.ntile(10).over(w2).alias("status_code_bucket"),
        F.sum("num_context").over(w3).alias("num_context_sum_2h"),
        F.lead("status_code", 2).over(w4).alias("next_status_code"),
        F.lead("status_code", 2).over(w5).alias("next_status_code_2"),
    )
)

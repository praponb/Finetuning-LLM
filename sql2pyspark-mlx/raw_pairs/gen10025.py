from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("log_ts")).orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("log_ts", "id").rowsBetween(-5, -1)
w_hist = Window.orderBy("status_code_high", "id")
w4 = Window.orderBy(F.col("status_code_high").desc(), "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.size("context").alias("num_context"),
    )
)

result = (
    parsed
    .filter(F.col("status_code_high") >= 1)
    .select(
        "id",
        "latency_ms",
        "status_code_high",
        "user_tier",
        F.sum("num_context").over(w).alias("running_num_context"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_5_prior"),
        F.ntile(4).over(w_hist).alias("status_code_high_bucket"),
        F.ntile(4).over(w4).alias("status_code_high_bucket_2"),
    )
)

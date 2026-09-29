from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("key").orderBy("status_code", "id", "key")
w4 = Window.partitionBy(F.to_date("log_ts"), "host")
w5 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "key",
        "v",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.min("latency_ms").over(w4).alias("log_ts_to_date_min_latency_ms"),
        F.sum("status_code").over(w5).alias("status_code_sum_15m"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("host")
w2 = Window.partitionBy(F.year("log_ts"), "host").orderBy("latency_ms", "id", "k").rowsBetween(-3, -1)
w3 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("host").orderBy("status_code", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "k",
        F.avg("latency_ms").over(w_grp).alias("host_avg_latency_ms"),
        F.avg("status_code").over(w2).alias("status_code_avg_3_prior"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_6"),
        F.lag("status_code", 1).over(w4).alias("prev_status_code"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key").rowsBetween(-2, -1)
w3 = Window.partitionBy(F.year("log_ts"), "host").orderBy("latency_ms", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "latency_ms",
        "attr_key",
        F.corr("latency_ms", "status_code").over(w).alias("corr_latency_ms_status_code"),
        F.avg("status_code").over(w2).alias("status_code_avg_2_prior"),
        F.lag("status_code", 1).over(w3).alias("prev_status_code"),
    )
)

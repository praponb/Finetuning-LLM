from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "k",
        F.sum("latency_ms").over(w).alias("latency_ms_sum_2h"),
    )
)

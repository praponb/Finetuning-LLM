from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("k")

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "log_ts",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.count("status_code").over(w).alias("status_code_count_2h"),
        F.max("latency_ms").over(w2).alias("k_max_latency_ms"),
    )
)

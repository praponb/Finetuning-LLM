from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "k")
w2 = Window.partitionBy("level")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    flattened
    .filter(F.col("level").isin(["WARN", "DEBUG", "ERROR"]))
    .select(
        "id",
        "log_ts",
        "level",
        "value",
        F.avg("latency_ms").over(w).alias("level_avg_latency_ms"),
        F.count("latency_ms").over(w2).alias("level_count_latency_ms"),
        F.count("status_code").over(w3).alias("status_code_count_30m"),
    )
)

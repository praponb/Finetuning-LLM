from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy(F.year("log_ts"), "service").orderBy("status_code", "id", "k")
w3 = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "service").orderBy("status_code", "id", "k").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("k").orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "log_ts",
        "k",
        "attr_value",
        F.count("status_code").over(w).alias("status_code_count_30m"),
        F.lag("latency_ms", 2).over(w2).alias("prev_latency_ms"),
        F.avg("status_code").over(w3).alias("status_code_avg_3"),
        F.lead("status_code", 1).over(w4).alias("next_status_code"),
        F.dense_rank().over(w5).alias("status_code_dense_rank"),
    )
)

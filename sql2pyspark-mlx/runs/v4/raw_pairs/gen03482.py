from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("service")
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k", "level")
w4 = Window.partitionBy("level").orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select(
        "*",
        F.count("status_code").over(w_hist).alias("service_count_status_code"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.sum("latency_ms").over(w3).alias("k_sum_latency_ms"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.percent_rank().over(w4).alias("status_code_percent_rank"),
    )
)

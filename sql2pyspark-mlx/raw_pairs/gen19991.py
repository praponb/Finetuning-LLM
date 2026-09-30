from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code")
w2 = Window.partitionBy(F.date_trunc("MONTH", "log_ts"), "service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .filter(F.col("host") == "h4")
    .select(
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "attr_value",
        F.percent_rank().over(w).alias("status_code_percent_rank"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
    )
)

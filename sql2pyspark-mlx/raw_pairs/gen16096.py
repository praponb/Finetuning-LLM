from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "key")
w2 = Window.partitionBy("host", "key")
w_running = Window.partitionBy(F.to_date("log_ts"), "service").orderBy("status_code")
w_part = Window.partitionBy("service").orderBy("log_ts", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .filter(F.col("key") != "version")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        "attr_value",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.rank().over(w_running).alias("status_code_rank"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

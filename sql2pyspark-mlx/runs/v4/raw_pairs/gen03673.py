from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("k", "host")
w_running_2 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(-2, -1)
w3 = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "*",
        F.corr("latency_ms", "status_code").over(w_running).alias("corr_latency_ms_status_code"),
        F.avg("status_code").over(w_running_2).alias("status_code_avg_2_prior"),
        F.last("k", ignorenulls=True).over(w3).alias("last_known_k"),
    )
)

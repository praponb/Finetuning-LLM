from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(-6, Window.currentRow)
w_running = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.count("status_code").over(w_grp).alias("last7_count_status_code"),
        F.max("status_code").over(w_grp).alias("last7_max_status_code"),
        F.sum("status_code").over(w_grp).alias("last7_sum_status_code"),
        F.first("latency_ms", ignorenulls=True).over(w_running).alias("first_known_latency_ms"),
    )
)

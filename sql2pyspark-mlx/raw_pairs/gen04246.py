from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("status_code")
w2 = Window.partitionBy("key")
w_grp = Window.partitionBy("key", "level")

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "level",
        "key",
        "v",
        F.cume_dist().over(w_recent).alias("status_code_cume_dist"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.corr("status_code", "latency_ms").over(w_grp).alias("corr_status_code_latency_ms_2"),
    )
)

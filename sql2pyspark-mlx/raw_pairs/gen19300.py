from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("host")
w2 = Window.orderBy(F.col("latency_ms").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select(
        "host",
        "latency_ms",
        "attr_value",
        F.sum("latency_ms").over(w_grp).alias("host_sum_latency_ms"),
        F.cume_dist().over(w2).alias("latency_ms_cume_dist"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .filter(F.col("latency_ms") > 2365)
    .select(
        "status_code",
        "value",
        F.rank().over(w_part).alias("status_code_rank"),
        F.corr("latency_ms", "status_code").over(w2).alias("corr_latency_ms_status_code"),
    )
)

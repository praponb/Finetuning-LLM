from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code")
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .filter(F.col("service") != "checkout")
    .select(
        "level",
        "latency_ms",
        "status_code",
        "k",
        F.rank().over(w).alias("status_code_rank"),
        F.rank().over(w2).alias("latency_ms_rank"),
    )
)

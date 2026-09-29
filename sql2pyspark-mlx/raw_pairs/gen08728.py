from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("k", "host").orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .filter(F.col("latency_ms") > 1152)
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.rank().over(w_hist).alias("status_code_rank"),
    )
)

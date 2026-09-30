from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level", "k").orderBy("latency_ms", "id", "k")

lines = (
    df_app_logs
    .select("id", "level", "latency_ms", F.explode("context").alias("k", "v"))
)

result = (
    lines
    .filter(F.col("k").isin(["region", "trace"]))
    .select(
        "id",
        "k",
        F.lag("latency_ms").over(w_running).alias("prev_latency_ms"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

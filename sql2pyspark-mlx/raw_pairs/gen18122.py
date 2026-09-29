from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "k").orderBy("latency_ms", "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    lines
    .filter(~F.col("service").isin(["search", "auth", "billing"]))
    .withColumn("next_latency_ms", F.lead("latency_ms").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

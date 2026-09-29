from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("attr_key", "host").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .filter(F.col("host").isin(["h3", "h4", "h2"]))
    .select(
        "id",
        "host",
        "level",
        "attr_key",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_3"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w3).alias("latency_ms_bucket"),
    )
)

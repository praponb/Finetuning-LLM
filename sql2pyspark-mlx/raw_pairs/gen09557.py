from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("level")
w_part = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key").rowsBetween(-2, -1)
w_part_2 = Window.partitionBy("service").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w4 = Window.partitionBy("host", "level")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .filter(F.col("host") != "h1")
    .select(
        "id",
        "service",
        "host",
        "level",
        "attr_key",
        F.stddev_samp("latency_ms").over(w_ordered).alias("level_stddev_samp_latency_ms"),
        F.avg("latency_ms").over(w_part).alias("latency_ms_avg_2_prior"),
        F.ntile(3).over(w_part_2).alias("latency_ms_bucket"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
    )
)

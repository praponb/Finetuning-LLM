from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy("latency_ms", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .filter(F.col("level") == "ERROR")
    .select(
        "id",
        "level",
        "latency_ms",
        "key",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.lag("latency_ms", 1).over(w2).alias("prev_latency_ms"),
    )
)

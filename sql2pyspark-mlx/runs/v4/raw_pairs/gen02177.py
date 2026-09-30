from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id")
w2 = Window.partitionBy("level")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.col("context")["version"].alias("version"),
        F.size("context").alias("num_context"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.col("context").getItem("region").alias("region"),
    )
)

result = (
    flattened_attrs
    .withColumn("prev_num_context", F.lag("num_context", 2, 0).over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
)

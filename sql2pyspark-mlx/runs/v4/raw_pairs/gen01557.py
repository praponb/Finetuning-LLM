from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")
w3 = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .withColumn("last_known_latency_ms", F.last("latency_ms", ignorenulls=True).over(w_recent))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms").desc(), "id", "key")
w2 = Window.partitionBy("service").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    lines
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("cumulative_latency_ms", F.sum("latency_ms").over(w2))
)

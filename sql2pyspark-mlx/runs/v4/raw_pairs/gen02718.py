from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host").orderBy("latency_ms", "id", "k").rowsBetween(-2, -1)
w2 = Window.partitionBy("host", "service").orderBy("latency_ms", "id", "k")
w3 = Window.partitionBy("service").orderBy("latency_ms", "id", "k").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy(F.col("latency_ms").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_2_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_3"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

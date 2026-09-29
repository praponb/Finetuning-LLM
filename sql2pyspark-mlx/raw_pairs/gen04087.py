from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id")
w2 = Window.orderBy(F.col("latency_ms").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "context",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "latency_ms",
        "has_region",
        F.row_number().over(w).alias("row_seq"),
        F.lag("latency_ms", 1).over(w).alias("prev_latency_ms"),
        F.lead("latency_ms", 1).over(w).alias("next_latency_ms"),
        F.lag("latency_ms", 1).over(w).alias("prev_latency_ms_2"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

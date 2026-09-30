from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("num_context").desc_nulls_last())
w_grp = Window.partitionBy("level", "service").orderBy("latency_ms", "id")
w4 = Window.partitionBy("service").orderBy(F.col("num_context").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.size("context").alias("num_context"),
        F.col("context").getItem("trace").alias("trace"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("region").alias("region"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "latency_ms",
        "num_context",
        "trace",
        "user_tier",
        F.sum("latency_ms").over(w).alias("cumulative_latency_ms"),
        F.dense_rank().over(w2).alias("num_context_dense_rank"),
        F.lag("num_context", 1).over(w_grp).alias("prev_num_context"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

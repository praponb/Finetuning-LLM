from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code_high").desc_nulls_last())
w2 = Window.partitionBy("service").orderBy(F.col("is_flagged").desc_nulls_last())
w3 = Window.orderBy(F.col("is_flagged").desc_nulls_last())
w4 = Window.orderBy(F.col("latency_ms").desc())
w5 = Window.partitionBy("service")
w6 = Window.orderBy(F.col("is_flagged_dense_rank").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    exploded
    .filter(F.col("service") == "billing")
    .select(
        "service",
        "latency_ms",
        "key",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
    )
)

with_windows = (
    unpacked
    .select(
        "*",
        F.rank().over(w).alias("status_code_high_rank"),
        F.dense_rank().over(w2).alias("is_flagged_dense_rank"),
        F.percent_rank().over(w3).alias("is_flagged_percent_rank"),
        F.percent_rank().over(w4).alias("latency_ms_percent_rank"),
        F.avg("status_code_high").over(w5).alias("service_avg_status_code_high"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
        F.percent_rank().over(w6).alias("is_flagged_dense_rank_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "key",
        "attr_value",
        "status_code_high",
        "is_flagged",
        "status_code_high_rank",
        "is_flagged_dense_rank",
        "is_flagged_percent_rank",
        "service_avg_status_code_high",
        "latency_ms_cume_dist",
    )
)

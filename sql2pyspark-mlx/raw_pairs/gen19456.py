from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("has_fault").asc_nulls_first())
w2 = Window.orderBy("has_fault")
w3 = Window.orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("key")
w5 = Window.partitionBy("key").orderBy("has_fault_cume_dist")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

with_windows = (
    parsed
    .select(
        "status_code",
        "key",
        F.cume_dist().over(w).alias("has_fault_cume_dist"),
        F.percent_rank().over(w2).alias("has_fault_percent_rank"),
        F.percent_rank().over(w3).alias("status_code_percent_rank"),
    )
)

metrics = (
    with_windows
    .filter(F.col("key").isin(["trace", "region"]))
    .select(
        "status_code",
        "key",
        F.min("status_code_percent_rank").over(w4).alias("key_min_status_code_percent_rank"),
        F.dense_rank().over(w5).alias("has_fault_cume_dist_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "status_code",
        "key",
        "key_min_status_code_percent_rank",
        "has_fault_cume_dist_dense_rank",
    )
    .orderBy(F.col("has_fault_cume_dist_dense_rank").desc_nulls_first(), "key")
)

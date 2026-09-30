from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code_per_latency_ms", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

base = (
    exploded
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        "attr_value",
        F.round((F.col("status_code") / F.col("latency_ms")), 2).alias("status_code_per_latency_ms"),
        F.length("k").alias("k_length"),
    )
)

base_app_logs = (
    base
    .filter(F.col("k_length") == 58)
    .select(
        "*",
        (F.col("k_length") * F.col("status_code_per_latency_ms")).alias("k_length_times_status_code_per_latency_ms"),
    )
)

latest = (
    base_app_logs
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "latency_ms",
        "status_code",
        "k",
        "status_code_per_latency_ms",
        "k_length",
        "k_length_times_status_code_per_latency_ms",
        "latest_rank",
    )
    .orderBy(F.col("status_code_per_latency_ms").desc_nulls_first())
)

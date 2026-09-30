from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy(F.col("latency_ms").desc())
w3 = Window.orderBy(F.col("latency_ms_dense_rank").desc_nulls_last())
w4 = Window.orderBy("latency_ms")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    lines
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "k",
        "attr_value",
        F.substring("k", 1, 4).alias("k_prefix"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("service") == "auth")
    .select(
        "*",
        F.upper("level").alias("level_upper"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

enriched = (
    cleaned_app_logs
    .select(
        "id",
        "latency_ms",
        F.sum("latency_ms").over(w).alias("service_sum_latency_ms"),
    )
)

metrics = enriched.select("*", F.dense_rank().over(w2).alias("latency_ms_dense_rank"))

scored = (
    metrics
    .withColumn("latency_ms_dense_rank_rank", F.rank().over(w3))
    .withColumn("latency_ms_dense_rank_2", F.dense_rank().over(w4))
)

filtered = (
    scored
    .filter(F.col("service_sum_latency_ms") >= 6176)
    .select(
        "id",
        "latency_ms",
        "service_sum_latency_ms",
        "latency_ms_dense_rank_rank",
        "latency_ms_dense_rank_2",
        (F.col("latency_ms") + F.col("latency_ms_dense_rank")).alias("latency_ms_plus_latency_ms_dense_rank"),
        F.when(F.col("latency_ms") >= 2382, "High").when(F.col("latency_ms") >= 937, "Medium").otherwise("Low").alias("latency_ms_level"),
    )
)

result = (
    filtered
    .select(
        "id",
        "latency_ms_dense_rank_rank",
        "latency_ms_dense_rank_2",
        "latency_ms_plus_latency_ms_dense_rank",
        "latency_ms_level",
    )
    .orderBy(F.col("latency_ms_dense_rank_rank").desc_nulls_last())
)

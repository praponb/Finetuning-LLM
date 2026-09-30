from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("service")
w2 = Window.orderBy(F.col("latency_ms").desc())
w3 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc_nulls_first())
w4 = Window.partitionBy("level", "host")
w5 = Window.partitionBy("service").orderBy(F.col("latency_ms_share_2").desc_nulls_first())
w6 = Window.partitionBy("service").orderBy(F.col("latency_ms_dense_rank_2").asc_nulls_last())
w7 = Window.partitionBy("service").orderBy("service_min_latency_ms")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("attr_key").isNull() & F.col("attr_key").isNotNull())
    .select(
        "*",
        F.lower("level").alias("level_lower"),
        F.trim("level").alias("clean_level"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_clean"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        F.min("latency_ms").over(w_ordered).alias("service_min_latency_ms"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w_ordered)), 4).alias("latency_ms_share"),
        F.dense_rank().over(w3).alias("latency_ms_dense_rank_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share_2"),
    )
)

metrics = (
    with_windows
    .withColumn("latency_ms_dense_rank_2_share", F.round((F.col("latency_ms_dense_rank_2") / F.sum("latency_ms_dense_rank_2").over(w_ordered)), 4))
    .withColumn("latency_ms_dense_rank_2_share_2", F.round((F.col("latency_ms_dense_rank_2") / F.sum("latency_ms_dense_rank_2").over(w_ordered)), 4))
    .withColumn("latency_ms_share_2_percent_rank", F.percent_rank().over(w5))
    .withColumn("latency_ms_dense_rank_2_dense_rank", F.dense_rank().over(w6))
    .withColumn("service_min_latency_ms_rank", F.rank().over(w7))
)

filtered = (
    metrics
    .filter(F.col("latency_ms_share_2_percent_rank") < 0.57)
    .withColumn("service_min_latency_ms_rank_per_latency_ms_dense_rank_2_share", (F.col("service_min_latency_ms_rank") / F.nullif("latency_ms_dense_rank_2_share", F.lit(0))))
    .withColumn("is_checkout", F.when(F.col("service") == "checkout", 1).otherwise(0))
)

result = (
    filtered
    .select(
        "id",
        "service",
        "service_min_latency_ms",
        "latency_ms_share",
        "latency_ms_dense_rank_2",
        "latency_ms_dense_rank_2_share_2",
        "latency_ms_dense_rank_2_dense_rank",
        "service_min_latency_ms_rank_per_latency_ms_dense_rank_2_share",
    )
    .orderBy(
        F.col("latency_ms_dense_rank_2").desc_nulls_last(),
        "service_min_latency_ms",
    )
)

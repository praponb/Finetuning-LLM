from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("has_fault", "id", "key")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("key").orderBy(F.col("status_code_share").desc())
w4 = Window.partitionBy("key").orderBy(F.col("status_code_share").desc_nulls_last())
w5 = Window.partitionBy("service").orderBy(F.col("next_latency_ms").desc_nulls_last())
w6 = Window.orderBy(F.col("has_fault").desc_nulls_first())
w7 = Window.partitionBy("status_code_share_percent_rank_tier").orderBy(F.col("trace_events").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

unpacked = (
    flattened
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
    )
)

metrics = (
    unpacked
    .filter(F.col("key").isin(["trace", "version", "region"]))
    .select(
        "*",
        F.lead("latency_ms", 1, 0).over(w).alias("next_latency_ms"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
    )
)

remaining_keys = (
    metrics
    .select("id", "service")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["DEBUG", "INFO", "WARN"]))
        .select("id", "service")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

metrics_app_logs = (
    kept_rows
    .withColumn("status_code_share_dense_rank", F.dense_rank().over(w3))
    .withColumn("status_code_share_percent_rank", F.percent_rank().over(w4))
    .withColumn("next_latency_ms_dense_rank", F.dense_rank().over(w5))
    .withColumn("has_fault_cume_dist", F.cume_dist().over(w6))
)

result = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("status_code_share_percent_rank") >= 0.85, "PREMIUM").otherwise("STANDARD").alias("status_code_share_percent_rank_tier"),
        "key",
    )
    .agg(
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_rows"),
        F.count_if(F.col("key") == "trace").alias("trace_events"),
        F.count("*").alias("event_count"),
    )
    .withColumn("trace_events_rank", F.rank().over(w7))
    .orderBy(F.col("key").desc_nulls_first())
)

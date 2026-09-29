from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("service")
w3 = Window.orderBy(F.col("latency_ms_dense_rank").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

filtered = (
    flattened
    .filter(F.col("latency_ms") >= 2002)
    .withColumn("service_upper", F.upper("service"))
)

cleaned = (
    filtered
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "latency_ms",
        "v",
        F.length("attr_key").alias("attr_key_length"),
        F.upper("attr_key").alias("attr_key_upper"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key"),
    )
)

valid = (
    cleaned
    .filter(F.col("service").isin(["billing", "checkout"]))
    .select(
        "service",
        "latency_ms",
        "attr_key_length",
        "resolved_attr_key",
        F.when(F.col("attr_key_length") >= 82, "High").when(F.col("attr_key_length") >= 30, "Medium").otherwise("Low").alias("attr_key_length_tier"),
    )
)

with_windows = (
    valid
    .filter(F.col("latency_ms").between(850, 2246))
    .select(
        "service",
        "latency_ms",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
    )
)

scored = (
    with_windows
    .select(
        "service",
        "latency_ms",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share_2"),
        F.percent_rank().over(w3).alias("latency_ms_dense_rank_percent_rank"),
    )
)

remaining_keys = (
    scored
    .select("service")
    .intersectAll(
        df_app_logs
        .filter(F.col("latency_ms").between(1195, 1978))
        .select("service")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .groupBy(
        F.when(F.col("latency_ms") >= 2228, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.sum(F.when(F.col("service") == "auth", F.col("latency_ms_dense_rank_percent_rank")).otherwise(0)).alias("auth_latency_ms_dense_rank_percent_rank"),
        F.count_if(F.col("service") == "auth").alias("auth_events"),
    )
    .filter(F.col("auth_events") > 1)
)

result = stats.select("latency_ms_tier", "latency_ms_bit_or")

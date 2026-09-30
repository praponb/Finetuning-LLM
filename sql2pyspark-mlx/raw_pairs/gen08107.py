from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("is_trace").desc_nulls_last())
w2 = Window.partitionBy("k").orderBy("latency_ms")
w3 = Window.orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("k")
w5 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w_grp = Window.orderBy(F.col("is_trace").desc())

exploded = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode_outer("context").alias("k", "v"))
)

valid = (
    exploded
    .filter(F.col("k").isNotNull() & (~F.col("k").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "latency_ms",
        "k",
        "v",
        F.initcap(F.trim("host")).alias("host_title"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

filtered = (
    valid
    .filter(F.col("k").isNotNull() & F.col("k").isin(["region", "version"]))
    .select(
        "id",
        "latency_ms",
        "k",
        "host_title",
        "k_clean",
        F.when(F.col("k") == "trace", 1).otherwise(0).alias("is_trace"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.to_date("log_ts") < "2023-10-01")
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(~F.col("k").isin(["trace", "version"]))
    .select(
        "id",
        "latency_ms",
        "host_title",
        "k_clean",
        "is_trace",
        F.cume_dist().over(w).alias("is_trace_cume_dist"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.dense_rank().over(w3).alias("latency_ms_dense_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
    )
)

enriched = (
    metrics
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w5))
    .withColumn("is_trace_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("is_trace_percent_rank", F.percent_rank().over(w_grp))
)

cleaned = (
    enriched
    .filter(F.col("is_trace") == 26)
    .select(
        "id",
        "latency_ms",
        "k_clean",
        "is_trace_cume_dist",
        "latency_ms_rank",
        "latency_ms_dense_rank",
        "latency_ms_percent_rank",
        "is_trace_dense_rank",
        "is_trace_percent_rank",
        F.coalesce("is_trace_cume_dist", F.lit(0)).alias("is_trace_cume_dist_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("is_trace_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("is_trace_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

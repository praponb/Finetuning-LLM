from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.partitionBy("service", "k")
w3 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("k", "service").orderBy("latency_ms")
w5 = Window.partitionBy("service").orderBy(F.col("latency_ms_dense_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("latency_ms_rank").desc_nulls_last())
w_ordered = Window.orderBy("latency_ms_dense_rank_dense_rank")
w8 = Window.partitionBy("k", "service")
w9 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select("id", "service", "latency_ms", F.explode_outer("context").alias("k", "v"))
)

with_windows = (
    exploded
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("service_min_latency_ms", F.min("latency_ms").over(w2))
    .withColumn("latency_ms_rank", F.rank().over(w3))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w4))
)

kept_keys = (
    with_windows
    .select("service", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("log_ts") < "2023-10-01")
        .select("service", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.service") == F.col("k.service")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.dense_rank().over(w5).alias("latency_ms_dense_rank_dense_rank"),
        F.cume_dist().over(w6).alias("latency_ms_rank_cume_dist"),
    )
)

with_windows_app_logs = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("latency_ms_dense_rank_dense_rank_cume_dist"),
        F.round((F.col("service_min_latency_ms") / F.sum("service_min_latency_ms").over(w8)), 4).alias("service_min_latency_ms_share"),
    )
)

enriched = (
    with_windows_app_logs
    .select(
        "*",
        F.count("latency_ms_dense_rank_dense_rank").over(w2).alias("service_count_latency_ms_dense_rank_dense_rank"),
        F.avg("latency_ms_rank_cume_dist").over(w9).alias("service_avg_latency_ms_rank_cume_dist"),
    )
)

result = (
    enriched
    .groupBy("service")
    .pivot("k", ["region", "trace"])
    .agg(F.sum("service_min_latency_ms_share").alias("total"))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("host", "level").orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").asc_nulls_last())

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

metrics = (
    flattened
    .select(
        "id",
        "host",
        F.rank().over(w).alias("latency_ms_rank"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
    )
)

cleaned = (
    metrics
    .filter(F.col("host") == "h3")
    .select(
        "*",
        (F.col("latency_ms_dense_rank") - F.col("latency_ms_cume_dist")).alias("latency_ms_dense_rank_minus_latency_ms_cume_dist"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("latency_ms_dense_rank") == 5)
    .select(
        "*",
        (F.col("latency_ms_rank") * F.col("latency_ms_dense_rank")).alias("latency_ms_rank_times_latency_ms_dense_rank"),
    )
)

result = (
    cleaned_app_logs
    .groupBy(
        F.when(F.col("latency_ms_cume_dist") >= 0.62, "HIGH").otherwise("LOW").alias("latency_ms_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms_cume_dist", [0.25, 0.5, 0.75]).alias("latency_ms_cume_dist_quartiles"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_count"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
)

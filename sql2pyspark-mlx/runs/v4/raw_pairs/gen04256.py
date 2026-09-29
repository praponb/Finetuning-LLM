from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("latency_ms_rounded")
w2 = Window.orderBy(F.col("latency_ms_rounded").desc_nulls_first())
w3 = Window.orderBy(F.col("latency_ms_rounded").desc_nulls_last())
w4 = Window.orderBy(F.col("latency_ms_rounded_dense_rank").asc_nulls_last())
w5 = Window.orderBy(F.col("latency_ms_rounded_dense_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("latency_ms_rounded_dense_rank_dense_rank").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("attr_key").isNotNull() & F.col("host").isin(["h2", "h4", "h3"]))
    .select(
        "id",
        "attr_value",
        F.round("latency_ms", 2).alias("latency_ms_rounded"),
        F.when(F.col("host").isin(["h4", "h3"]), "Group A").otherwise("Other").alias("host_group"),
    )
)

scored = (
    valid
    .withColumn("latency_ms_rounded_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("latency_ms_rounded_dense_rank_2", F.dense_rank().over(w2))
    .withColumn("latency_ms_rounded_cume_dist", F.cume_dist().over(w3))
)

metrics = (
    scored
    .select(
        "*",
        F.dense_rank().over(w4).alias("latency_ms_rounded_dense_rank_dense_rank"),
        F.cume_dist().over(w5).alias("latency_ms_rounded_dense_rank_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w2).alias("latency_ms_rounded_percent_rank"),
        F.percent_rank().over(w6).alias("latency_ms_rounded_dense_rank_dense_rank_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("latency_ms_rounded").isNotNull())
    .select(
        "id",
        "attr_value",
        "latency_ms_rounded",
        "host_group",
        "latency_ms_rounded_dense_rank",
        "latency_ms_rounded_dense_rank_2",
        "latency_ms_rounded_dense_rank_dense_rank",
        "latency_ms_rounded_dense_rank_cume_dist",
        "latency_ms_rounded_percent_rank",
        "latency_ms_rounded_dense_rank_dense_rank_percent_rank",
        F.ceil("latency_ms_rounded_dense_rank_cume_dist").alias("latency_ms_rounded_dense_rank_cume_dist_whole"),
        F.least("latency_ms_rounded_dense_rank_dense_rank", "latency_ms_rounded").alias("least_latency_ms_rounded_dense_rank_dense_rank_latency_ms_rounded"),
    )
)

valid_app_logs = (
    cleaned
    .filter(F.col("latency_ms_rounded_dense_rank_2") > 8)
    .select(
        "id",
        "attr_value",
        "latency_ms_rounded",
        "host_group",
        "latency_ms_rounded_dense_rank_2",
        "latency_ms_rounded_dense_rank_dense_rank",
        "latency_ms_rounded_percent_rank",
        "latency_ms_rounded_dense_rank_cume_dist_whole",
        "least_latency_ms_rounded_dense_rank_dense_rank_latency_ms_rounded",
        (F.col("latency_ms_rounded_dense_rank_2") / F.nullif("latency_ms_rounded_dense_rank_dense_rank_percent_rank", F.lit(0))).alias("latency_ms_rounded_dense_rank_2_per_latency_ms_rounded_dense_rank_dense_rank_percent_rank"),
        F.round("latency_ms_rounded_percent_rank", 1).alias("latency_ms_rounded_percent_rank_rounded"),
    )
)

result = (
    valid_app_logs
    .select(
        "id",
        "attr_value",
        "latency_ms_rounded",
        "host_group",
        "latency_ms_rounded_dense_rank_2",
        "latency_ms_rounded_dense_rank_dense_rank",
        "latency_ms_rounded_dense_rank_cume_dist_whole",
        "latency_ms_rounded_dense_rank_2_per_latency_ms_rounded_dense_rank_dense_rank_percent_rank",
        "latency_ms_rounded_percent_rank_rounded",
    )
    .orderBy(
        F.col("latency_ms_rounded_dense_rank_cume_dist_whole").asc_nulls_first(),
        F.col("latency_ms_rounded").desc(),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "level").orderBy("latency_ms", "id", "k").rowsBetween(-3, -1)
w2 = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("level")
w_running = Window.partitionBy("k").orderBy(F.col("latency_ms_avg_3_prior").desc(), "id", "k")
w_grp = Window.partitionBy("k", "level").orderBy("latency_ms_avg_3_prior", "id", "k").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("level").orderBy("latency_ms_rank_plus_latency_ms", "id", "k")
w7 = Window.orderBy("prev_latency_ms_avg_3_prior", "id", "k")
w8 = Window.partitionBy("k").orderBy("prev_latency_ms_avg_3_prior", "id", "k")
w9 = Window.partitionBy("k").orderBy(F.col("latency_ms_rank").desc(), "id", "k")
w10 = Window.orderBy(F.col("latency_ms_avg_3_prior_bucket").desc(), "id", "k")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

cleaned = (
    lines
    .filter(F.col("latency_ms") <= 1200)
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        "value",
        F.concat_ws("-", "k", "level").alias("k_level_key"),
        F.upper("level").alias("level_upper"),
    )
)

enriched = (
    cleaned
    .withColumn("latency_ms_avg_3_prior", F.avg("latency_ms").over(w))
    .withColumn("latency_ms_rank", F.rank().over(w2))
    .withColumn("level_avg_latency_ms", F.avg("latency_ms").over(w3))
)

valid = (
    enriched
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "level",
        "k",
        "value",
        "level_upper",
        "latency_ms_avg_3_prior",
        "latency_ms_rank",
        "level_avg_latency_ms",
        (F.col("latency_ms_rank") + F.col("latency_ms")).alias("latency_ms_rank_plus_latency_ms"),
        F.coalesce("k", F.lit("OTHER")).alias("resolved_k"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.ntile(5).over(w_running).alias("latency_ms_avg_3_prior_bucket"),
        F.avg("latency_ms_rank_plus_latency_ms").over(w_grp).alias("latency_ms_rank_plus_latency_ms_avg_3"),
        F.lag("latency_ms_avg_3_prior", 1, 0).over(w6).alias("prev_latency_ms_avg_3_prior"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lag("level_avg_latency_ms", 2).over(w7).alias("prev_level_avg_latency_ms"),
        F.ntile(4).over(w8).alias("prev_latency_ms_avg_3_prior_bucket"),
    )
)

result = (
    scored
    .withColumn("row_seq", F.row_number().over(w9))
    .withColumn("row_seq_2", F.row_number().over(w10))
)

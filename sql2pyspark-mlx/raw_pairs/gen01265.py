from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("level")
w4 = Window.orderBy(F.col("status_code_share").desc())
w5 = Window.orderBy(F.col("status_code_share_2").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "v",
        F.rank().over(w).alias("status_code_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_2"),
    )
)

enriched = (
    metrics
    .select(
        "status_code_rank",
        "status_code_share_2",
        F.cume_dist().over(w4).alias("status_code_share_cume_dist"),
        F.dense_rank().over(w4).alias("status_code_share_dense_rank"),
        F.rank().over(w5).alias("status_code_share_2_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("status_code_share_cume_dist").isNotNull())
    .select(
        "status_code_rank",
        "status_code_share_2",
        "status_code_share_dense_rank",
        "status_code_share_2_rank",
        (F.col("status_code_share_dense_rank") + F.col("status_code_share_cume_dist")).alias("status_code_share_dense_rank_plus_status_code_share_cume_dist"),
        (F.col("status_code_rank") / F.nullif("status_code_share_2", F.lit(0))).alias("status_code_rank_per_status_code_share_2"),
    )
)

grouped = (
    valid
    .groupBy(
        F.when(F.col("status_code_share_2") >= 0.75, "LARGE").otherwise("SMALL").alias("status_code_share_2_tier"),
        F.when(F.col("status_code_share_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("status_code_share_dense_rank_tier"),
    )
    .agg(
        F.avg("status_code_share_2_rank").alias("avg_status_code_share_2_rank"),
        F.sum("status_code_rank").alias("total_status_code_rank"),
        F.percentile_approx("status_code_share_2", [0.25, 0.5, 0.75]).alias("status_code_share_2_quartiles"),
        F.count("*").alias("event_count"),
    )
)

result = (
    grouped
    .select(
        "status_code_share_2_tier",
        "status_code_share_dense_rank_tier",
        "avg_status_code_share_2_rank",
        "total_status_code_rank",
        "status_code_share_2_quartiles",
    )
    .orderBy(F.col("total_status_code_rank").asc_nulls_first())
)

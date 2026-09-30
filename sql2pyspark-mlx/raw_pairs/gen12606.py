from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy(F.month("log_ts"), "k")
w4 = Window.orderBy(F.col("latency_ms_share").asc_nulls_last())
w5 = Window.partitionBy("k").orderBy(F.col("latency_ms_share").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

scored = (
    exploded
    .select(
        "latency_ms",
        "k",
        "attr_value",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.avg("status_code").over(w).alias("k_avg_status_code"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share_2"),
        F.sum("status_code").over(w2).alias("status_code_sum_1h"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share_3"),
    )
)

filtered = (
    scored
    .filter(F.col("latency_ms") >= 2007)
    .select(
        "k",
        "latency_ms_share",
        "k_avg_status_code",
        "latency_ms_share_3",
        F.col("latency_ms").cast("string").alias("latency_ms_str"),
        (F.col("latency_ms_share_3") + F.col("k_avg_status_code")).alias("latency_ms_share_3_plus_k_avg_status_code"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

enriched = (
    filtered
    .filter(F.col("latency_ms_share") < 0.24)
    .withColumn("latency_ms_share_dense_rank", F.dense_rank().over(w4))
    .withColumn("latency_ms_share_dense_rank_2", F.dense_rank().over(w5))
)

result = (
    enriched
    .select(
        "latency_ms_share",
        "k_avg_status_code",
        "latency_ms_share_3",
        "k_clean",
        "latency_ms_share_dense_rank_2",
    )
    .orderBy(F.col("k_avg_status_code").desc())
)

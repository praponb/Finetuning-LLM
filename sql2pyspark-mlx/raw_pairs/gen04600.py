from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w_part = Window.orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("key")
w_recent = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w5 = Window.partitionBy("status_code_rank_tier").orderBy(F.col("avg_latency_ms_share").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.corr("status_code", "latency_ms").over(w).alias("corr_status_code_latency_ms"),
        F.rank().over(w_part).alias("status_code_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4).alias("latency_ms_share"),
        F.corr("status_code", "latency_ms").over(w3).alias("corr_status_code_latency_ms_2"),
        F.percent_rank().over(w_recent).alias("latency_ms_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("latency_ms") <= 1061)
    .select(
        "id",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        "status_code_rank",
        "latency_ms_share",
        "corr_status_code_latency_ms_2",
        F.coalesce("latency_ms_share", F.lit(0)).alias("latency_ms_share_filled"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

filtered = (
    base
    .filter(F.col("key").isNotNull())
    .select(
        "status_code",
        "key",
        "attr_value",
        "status_code_rank",
        "latency_ms_share",
        "corr_status_code_latency_ms_2",
        "latency_ms_share_filled",
        "key_filled",
        F.when(F.col("latency_ms_share_filled") >= 81, "High").when(F.col("latency_ms_share_filled") >= 12, "Medium").otherwise("Low").alias("latency_ms_share_filled_band"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code_rank") >= 8, "HIGH").otherwise("LOW").alias("status_code_rank_tier"),
        F.when(F.col("latency_ms_share") >= 0.65, "HIGH").otherwise("LOW").alias("latency_ms_share_tier"),
    )
    .agg(
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.round(F.avg("latency_ms_share"), 2).alias("avg_latency_ms_share"),
    )
    .filter(F.col("avg_status_code") > 0)
    .withColumn("avg_latency_ms_share_rank", F.rank().over(w5))
)

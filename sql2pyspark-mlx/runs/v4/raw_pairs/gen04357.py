from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("attr_key")
w4 = Window.orderBy(F.col("latency_ms_count_15m").desc())
w5 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

metrics = (
    lines
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w))
    .withColumn("latency_ms_count_15m", F.count("latency_ms").over(w2))
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("latency_ms").over(w3).alias("attr_key_sum_latency_ms"),
        F.dense_rank().over(w4).alias("latency_ms_count_15m_dense_rank"),
        F.sum("latency_ms_dense_rank").over(w5).alias("latency_ms_dense_rank_sum_15m"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "*",
        F.max("latency_ms_dense_rank_sum_15m").over(w3).alias("attr_key_max_latency_ms_dense_rank_sum_15m"),
        F.max("latency_ms").over(w3).alias("attr_key_max_latency_ms"),
    )
)

valid = (
    enriched_app_logs
    .filter((F.col("log_ts") > F.lit("2025-06-01 09:00:00").cast("timestamp")) & (F.col("attr_key_sum_latency_ms") == 3066))
    .withColumn("latency_ms_dense_rank_sum_15m_filled", F.coalesce("latency_ms_dense_rank_sum_15m", F.lit(0)))
    .withColumn("attr_key_max_latency_ms_str", F.col("attr_key_max_latency_ms").cast("string"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("latency_ms_count_15m") >= 16, "HIGH").otherwise("LOW").alias("latency_ms_count_15m_tier"),
    )
    .agg(
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("latency_ms_dense_rank_sum_15m")).otherwise(0)).alias("region_latency_ms_dense_rank_sum_15m"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
        F.max("log_ts").alias("last_seen"),
        F.max(F.when(F.col("attr_key") == "region", F.col("attr_key_max_latency_ms_dense_rank_sum_15m"))).alias("region_max_attr_key_max_latency_ms_dense_rank_sum_15m"),
    )
)

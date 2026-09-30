from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "key")
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")
w5 = Window.partitionBy("key").orderBy("status_code_avg_5", "id", "key").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("key").orderBy(F.col("latency_ms_bucket_avg_3").desc())
w7 = Window.partitionBy("key").orderBy(F.col("key_min_status_code").desc_nulls_last(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

latest = (
    lines
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "latency_ms", "status_code", "key")
)

metrics = (
    latest
    .filter(F.col("key") == "version")
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.avg("status_code").over(w3).alias("status_code_avg_5"),
        F.ntile(4).over(w4).alias("latency_ms_bucket"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "key",
        "latency_ms_share",
        "latency_ms_bucket",
        F.avg("latency_ms_bucket").over(w5).alias("latency_ms_bucket_avg_3"),
        F.min("status_code").over(w2).alias("key_min_status_code"),
        F.max("latency_ms_bucket").over(w2).alias("key_max_latency_ms_bucket"),
    )
)

cleaned = (
    enriched
    .filter(F.col("key_max_latency_ms_bucket") == 4)
    .select(
        "id",
        "key",
        "latency_ms_bucket_avg_3",
        "key_min_status_code",
        (F.col("latency_ms_bucket") * F.col("key_max_latency_ms_bucket")).alias("latency_ms_bucket_times_key_max_latency_ms_bucket"),
    )
)

latest_app_logs = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_app_logs_2 = (
    latest_app_logs
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "key",
        "latency_ms_bucket_avg_3",
        "key_min_status_code",
        "latency_ms_bucket_times_key_max_latency_ms_bucket",
    )
)

summary = (
    latest_app_logs_2
    .groupBy(
        F.when(F.col("key_min_status_code") >= 440, "LARGE").otherwise("SMALL").alias("key_min_status_code_tier"),
    )
    .agg(
        F.count_if(F.col("key") == "trace").alias("trace_events"),
        F.percentile_approx("latency_ms_bucket_avg_3", [0.25, 0.5, 0.75]).alias("latency_ms_bucket_avg_3_quartiles"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.percentile_approx("key_min_status_code", 0.5).alias("key_min_status_code_p50"),
    )
)

result = summary.select("key_min_status_code_tier", "trace_events")
